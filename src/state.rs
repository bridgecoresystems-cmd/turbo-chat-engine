use crate::{fcm::FcmClient, proto::ChatMessage, storage::R2Storage};
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tracing::{error, info};

pub const RATE_LIMIT_MSG_PER_SEC: u32 = 10;

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    rooms: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
    /// room_id → set of all user_ids who ever joined (persists across reconnects)
    room_members: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// user_ids currently connected
    online_users: Arc<RwLock<HashSet<String>>>,
    redis_client: Client,
    redis_pub: Arc<Mutex<MultiplexedConnection>>,
    persist_tx: mpsc::Sender<ChatMessage>,
    pub jwt_secret: Arc<String>,
    pub pool: PgPool,
    pub storage: Arc<R2Storage>,
    pub fcm: Option<Arc<FcmClient>>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct RoomInfo {
    pub id:         String,
    pub name:       String,
    pub created_by: String,
    pub created_at: i64,
}

#[derive(Deserialize)]
pub struct CreateRoomRequest {
    pub name:    String,
    pub members: Vec<String>, // user_ids to invite
}

#[derive(Serialize)]
pub struct HistoryMessage {
    pub id: i64,
    pub room_id: String,
    pub sender_id: String,
    pub text: String,
    pub timestamp: i64,
    pub read_by_peer: bool,
}

impl AppState {
    pub async fn new(
        redis_url: &str,
        persist_tx: mpsc::Sender<ChatMessage>,
        jwt_secret: String,
        pool: PgPool,
        storage: R2Storage,
        fcm: Option<FcmClient>,
    ) -> Result<Self> {
        let redis_client = Client::open(redis_url)?;
        let redis_pub = redis_client.get_multiplexed_async_connection().await?;
        Ok(Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
            room_members: Arc::new(RwLock::new(HashMap::new())),
            online_users: Arc::new(RwLock::new(HashSet::new())),
            redis_client,
            redis_pub: Arc::new(Mutex::new(redis_pub)),
            persist_tx,
            jwt_secret: Arc::new(jwt_secret),
            pool,
            storage: Arc::new(storage),
            fcm: fcm.map(Arc::new),
        })
    }

    pub fn start_redis_dispatcher(&self) {
        let client = self.redis_client.clone();
        let rooms = self.rooms.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = redis_dispatcher(client.clone(), rooms.clone()).await {
                    error!("redis dispatcher crashed, restarting in 1s: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        });
    }

    pub async fn join_room(&self, room_id: &str) -> broadcast::Receiver<Bytes> {
        {
            let rooms = self.rooms.read().await;
            if let Some(tx) = rooms.get(room_id) {
                return tx.subscribe();
            }
        }
        let mut rooms = self.rooms.write().await;
        if let Some(tx) = rooms.get(room_id) {
            return tx.subscribe();
        }
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        rooms.insert(room_id.to_string(), tx);
        rx
    }

    pub async fn user_joined(&self, room_id: &str, user_id: &str) {
        self.room_members
            .write()
            .await
            .entry(room_id.to_string())
            .or_default()
            .insert(user_id.to_string());
        self.online_users.write().await.insert(user_id.to_string());
    }

    pub async fn user_left(&self, user_id: &str) {
        self.online_users.write().await.remove(user_id);
    }

    /// Returns user_ids of currently-online room members (excluding `exclude`).
    /// Used to inform a newly-joined user who else is already online.
    pub async fn online_room_members(&self, room_id: &str, exclude: &str) -> Vec<String> {
        let rm = self.room_members.read().await;
        let online = self.online_users.read().await;
        rm.get(room_id)
            .map(|members| {
                members.iter()
                    .filter(|u| u.as_str() != exclude && online.contains(u.as_str()))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns FCM tokens of room members who are offline (excluding sender).
    /// Uses the `contacts` table (keyed by dmRoomId) to find room members,
    /// because `room_members` uses UUID keys that don't match WebSocket room_ids.
    pub async fn offline_tokens(&self, room_id: &str, exclude: &str) -> Vec<String> {
        let online: Vec<String> = self.online_users.read().await.iter().cloned().collect();

        sqlx::query_scalar(
            "SELECT DISTINCT dt.fcm_token
             FROM contacts c
             JOIN device_tokens dt ON dt.user_id = c.contact_id
             WHERE c.room_id = $1
               AND c.contact_id != $2
               AND NOT (c.contact_id = ANY($3))",
        )
        .bind(room_id)
        .bind(exclude)
        .bind(&online[..])
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default()
    }

    /// Returns the FCM token for a single user, if registered.
    pub async fn fcm_token_for_user(&self, user_id: &str) -> Option<String> {
        sqlx::query_scalar::<_, String>(
            "SELECT fcm_token FROM device_tokens WHERE user_id = $1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None)
    }

    /// Returns all registered FCM tokens (for broadcast).
    pub async fn all_fcm_tokens(&self) -> Vec<String> {
        sqlx::query_scalar::<_, String>("SELECT fcm_token FROM device_tokens")
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default()
    }

    pub async fn publish(&self, room_id: &str, msg: Bytes) {
        let channel = format!("room:{room_id}");
        let mut conn = self.redis_pub.lock().await;
        let result: redis::RedisResult<()> = conn.publish(&channel, msg.as_ref()).await;
        if let Err(e) = result {
            error!("redis publish error on '{channel}': {e}");
        }
    }

    pub async fn record(&self, msg: ChatMessage) {
        let _ = self.persist_tx.try_send(msg);
    }

    /// Returns true if message was updated (sender matches and message not deleted).
    pub async fn edit_message(&self, message_id: u64, sender_id: &str, new_payload: &[u8]) -> bool {
        let now = now_ms();
        sqlx::query(
            "UPDATE messages SET payload = $1, edited_at = $2
             WHERE id = $3 AND sender_id = $4 AND deleted_at IS NULL",
        )
        .bind(new_payload)
        .bind(now)
        .bind(message_id as i64)
        .bind(sender_id)
        .execute(&self.pool)
        .await
        .map(|r| r.rows_affected() > 0)
        .unwrap_or(false)
    }

    /// Returns true if message was soft-deleted (sender matches and not already deleted).
    pub async fn delete_message(&self, message_id: u64, sender_id: &str) -> bool {
        let now = now_ms();
        sqlx::query(
            "UPDATE messages SET deleted_at = $1
             WHERE id = $2 AND sender_id = $3 AND deleted_at IS NULL",
        )
        .bind(now)
        .bind(message_id as i64)
        .bind(sender_id)
        .execute(&self.pool)
        .await
        .map(|r| r.rows_affected() > 0)
        .unwrap_or(false)
    }

    pub async fn record_read(&self, message_id: u64, user_id: &str, room_id: &str) {
        let now = now_ms();
        let _ = sqlx::query(
            "INSERT INTO read_receipts (message_id, user_id, room_id, read_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (message_id, user_id) DO NOTHING",
        )
        .bind(message_id as i64)
        .bind(user_id)
        .bind(room_id)
        .bind(now)
        .execute(&self.pool)
        .await;
    }

    pub async fn create_room(&self, req: CreateRoomRequest, created_by: &str) -> Result<RoomInfo> {
        let id  = uuid::Uuid::new_v4().to_string();
        let now = now_ms();

        sqlx::query(
            "INSERT INTO rooms (id, name, created_by, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(&id)
        .bind(&req.name)
        .bind(created_by)
        .bind(now)
        .execute(&self.pool)
        .await?;

        // Creator is always owner
        sqlx::query(
            "INSERT INTO room_members (room_id, user_id, role, joined_at) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(&id)
        .bind(created_by)
        .bind(now)
        .execute(&self.pool)
        .await?;

        for member in &req.members {
            let _ = sqlx::query(
                "INSERT INTO room_members (room_id, user_id, role, joined_at)
                 VALUES ($1, $2, 'member', $3)
                 ON CONFLICT DO NOTHING",
            )
            .bind(&id)
            .bind(member)
            .bind(now)
            .execute(&self.pool)
            .await;
        }

        Ok(RoomInfo { id, name: req.name, created_by: created_by.to_string(), created_at: now })
    }

    pub async fn list_rooms(&self, user_id: &str) -> Result<Vec<RoomInfo>> {
        let rows = sqlx::query_as::<_, RoomInfo>(
            "SELECT r.id, r.name, r.created_by, r.created_at
             FROM rooms r
             JOIN room_members rm ON rm.room_id = r.id
             WHERE rm.user_id = $1 AND r.deleted_at IS NULL
             ORDER BY r.created_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Returns true if room was deleted (only creator can delete).
    pub async fn delete_room(&self, room_id: &str, user_id: &str) -> bool {
        sqlx::query(
            "UPDATE rooms SET deleted_at = $1
             WHERE id = $2 AND created_by = $3 AND deleted_at IS NULL",
        )
        .bind(now_ms())
        .bind(room_id)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map(|r| r.rows_affected() > 0)
        .unwrap_or(false)
    }

    pub async fn get_history(&self, room_id: &str, limit: i64) -> Result<Vec<HistoryMessage>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            room_id: String,
            sender_id: String,
            payload: Vec<u8>,
            timestamp: i64,
            read_by_peer: bool,
        }

        // Get the newest `limit` real messages, exclude join/leave pseudo-messages,
        // join read_receipts to know if any non-sender has read each message,
        // then re-sort oldest-first so the client sees chronological order.
        let rows = sqlx::query_as::<_, Row>(
            "SELECT m.id, m.room_id, m.sender_id, m.payload, m.timestamp,
                    EXISTS(
                        SELECT 1 FROM read_receipts rr
                        WHERE rr.message_id = m.id AND rr.user_id != m.sender_id
                    ) AS read_by_peer
             FROM (
                 SELECT id, room_id, sender_id, payload, timestamp
                 FROM messages
                 WHERE room_id = $1
                   AND payload NOT IN ('join'::bytea, 'leave'::bytea)
                 ORDER BY timestamp DESC
                 LIMIT $2
             ) m
             ORDER BY m.timestamp ASC",
        )
        .bind(room_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| HistoryMessage {
                id: r.id,
                room_id: r.room_id,
                sender_id: r.sender_id,
                text: String::from_utf8(r.payload).unwrap_or_default(),
                timestamp: r.timestamp,
                read_by_peer: r.read_by_peer,
            })
            .collect())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn redis_dispatcher(
    client: Client,
    rooms: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
) -> Result<()> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.psubscribe("room:*").await?;
    info!("redis dispatcher ready (pattern: room:*)");

    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        let channel = msg.get_channel_name();
        let room_id = channel.strip_prefix("room:").unwrap_or(channel);

        if let Ok(data) = msg.get_payload::<Vec<u8>>() {
            let rooms = rooms.read().await;
            if let Some(tx) = rooms.get(room_id) {
                let _ = tx.send(Bytes::from(data));
            }
        }
    }

    Ok(())
}
