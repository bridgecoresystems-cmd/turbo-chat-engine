use crate::{fcm::FcmClient, proto::ChatMessage, storage::R2Storage};
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use serde::Serialize;
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

#[derive(Serialize)]
pub struct HistoryMessage {
    pub id: i64,
    pub room_id: String,
    pub sender_id: String,
    pub text: String,
    pub timestamp: i64,
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

    /// Returns FCM tokens of room members who are offline (excluding sender).
    pub async fn offline_tokens(&self, room_id: &str, exclude: &str) -> Vec<String> {
        let members: Vec<String> = {
            let rm = self.room_members.read().await;
            let online = self.online_users.read().await;
            rm.get(room_id)
                .map(|set| {
                    set.iter()
                        .filter(|u| u.as_str() != exclude && !online.contains(u.as_str()))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        };

        if members.is_empty() {
            return vec![];
        }

        sqlx::query_scalar(
            "SELECT fcm_token FROM device_tokens WHERE user_id = ANY($1)",
        )
        .bind(&members[..])
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

    pub async fn get_history(&self, room_id: &str, limit: i64) -> Result<Vec<HistoryMessage>> {
        struct Row {
            id: i64,
            room_id: String,
            sender_id: String,
            payload: Vec<u8>,
            timestamp: i64,
        }

        let rows = sqlx::query_as!(
            Row,
            "SELECT id, room_id, sender_id, payload, timestamp
             FROM messages
             WHERE room_id = $1
             ORDER BY timestamp ASC
             LIMIT $2",
            room_id,
            limit,
        )
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
