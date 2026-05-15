use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::error;

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    rooms: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
    redis_client: Client,
    redis_pub: Arc<Mutex<MultiplexedConnection>>,
}

impl AppState {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let redis_client = Client::open(redis_url)?;
        let redis_pub = redis_client.get_multiplexed_async_connection().await?;
        Ok(Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
            redis_client,
            redis_pub: Arc::new(Mutex::new(redis_pub)),
        })
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
        rooms.insert(room_id.to_string(), tx.clone());

        // One Redis subscriber per room — feeds the local broadcast channel
        let client = self.redis_client.clone();
        let room = room_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = redis_room_subscriber(client, room, tx).await {
                error!("redis subscriber crashed: {e}");
            }
        });

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
}

// Runs as a background task for each room.
// Subscribes to Redis channel and forwards messages into the local broadcast.
async fn redis_room_subscriber(
    client: Client,
    room_id: String,
    tx: broadcast::Sender<Bytes>,
) -> Result<()> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(format!("room:{room_id}")).await?;
    let mut stream = pubsub.on_message();

    while let Some(msg) = stream.next().await {
        let data: Vec<u8> = msg.get_payload()?;
        let _ = tx.send(Bytes::from(data)); // error = no local subscribers, safe to ignore
    }

    Ok(())
}
