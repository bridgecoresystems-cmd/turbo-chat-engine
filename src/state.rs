use crate::{proto::ChatMessage, storage::R2Storage};
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use serde::Serialize;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tracing::{error, info};

// Max messages per user per second
pub const RATE_LIMIT_MSG_PER_SEC: u32 = 10;

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    rooms: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
    redis_client: Client,
    redis_pub: Arc<Mutex<MultiplexedConnection>>,
    persist_tx: mpsc::Sender<ChatMessage>,
    pub jwt_secret: Arc<String>,
    pub pool: PgPool,
    pub storage: Arc<R2Storage>,
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
    ) -> Result<Self> {
        let redis_client = Client::open(redis_url)?;
        let redis_pub = redis_client.get_multiplexed_async_connection().await?;
        Ok(Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
            redis_client,
            redis_pub: Arc::new(Mutex::new(redis_pub)),
            persist_tx,
            jwt_secret: Arc::new(jwt_secret),
            pool,
            storage: Arc::new(storage),
        })
    }

    pub fn start_redis_dispatcher(&self) {
        let client = self.redis_client.clone();
        let rooms  = self.rooms.clone();
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

    pub async fn record_read(&self, message_id: u64, user_id: &str, room_id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
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
        struct Row { id: i64, room_id: String, sender_id: String, payload: Vec<u8>, timestamp: i64 }

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
