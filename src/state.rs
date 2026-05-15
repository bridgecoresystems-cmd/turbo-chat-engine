use bytes::Bytes;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, RwLock};

const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    rooms: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn join_room(&self, room_id: &str) -> broadcast::Receiver<Bytes> {
        {
            let rooms = self.rooms.read().await;
            if let Some(tx) = rooms.get(room_id) {
                return tx.subscribe();
            }
        }
        let mut rooms = self.rooms.write().await;
        // double-check after acquiring write lock
        if let Some(tx) = rooms.get(room_id) {
            return tx.subscribe();
        }
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        rooms.insert(room_id.to_string(), tx);
        rx
    }

    pub async fn publish(&self, room_id: &str, msg: Bytes) {
        let rooms = self.rooms.read().await;
        if let Some(tx) = rooms.get(room_id) {
            let _ = tx.send(msg); // ignore error if no receivers yet
        }
    }
}
