use crate::proto::ChatMessage;
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tracing::{error, info};

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    rooms: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
    redis_client: Client,
    redis_pub: Arc<Mutex<MultiplexedConnection>>,
    persist_tx: mpsc::Sender<ChatMessage>,
}

impl AppState {
    pub async fn new(redis_url: &str, persist_tx: mpsc::Sender<ChatMessage>) -> Result<Self> {
        let redis_client = Client::open(redis_url)?;
        let redis_pub = redis_client.get_multiplexed_async_connection().await?;
        Ok(Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
            redis_client,
            redis_pub: Arc::new(Mutex::new(redis_pub)),
            persist_tx,
        })
    }

    // Одна фоновая задача на весь сервер вместо одной на каждую комнату
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
        // больше не спавним Redis-задачу на каждую комнату
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
}

// Одно соединение, подписанное на паттерн "room:*".
// Все сообщения всех комнат приходят сюда и диспатчатся в нужный broadcast-канал.
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
