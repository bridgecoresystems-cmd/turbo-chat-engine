use crate::proto::ChatMessage;
use sqlx::PgPool;
use tokio::{
    sync::mpsc,
    time::{interval, Duration},
};
use tracing::{error, info};

const BATCH_SIZE: usize = 1000;
const FLUSH_SECS: u64 = 2;

pub struct BatchWorker {
    rx: mpsc::Receiver<ChatMessage>,
    pool: PgPool,
}

impl BatchWorker {
    pub fn new(rx: mpsc::Receiver<ChatMessage>, pool: PgPool) -> Self {
        Self { rx, pool }
    }

    pub async fn run(mut self) {
        let mut buf: Vec<ChatMessage> = Vec::with_capacity(BATCH_SIZE);
        let mut ticker = interval(Duration::from_secs(FLUSH_SECS));
        ticker.tick().await; // skip the immediate first tick

        loop {
            tokio::select! {
                msg = self.rx.recv() => match msg {
                    Some(m) => {
                        buf.push(m);
                        if buf.len() >= BATCH_SIZE {
                            flush(&self.pool, &mut buf).await;
                        }
                    }
                    None => {
                        // sender dropped — flush what's left and stop
                        if !buf.is_empty() {
                            flush(&self.pool, &mut buf).await;
                        }
                        break;
                    }
                },

                _ = ticker.tick() => {
                    if !buf.is_empty() {
                        flush(&self.pool, &mut buf).await;
                    }
                }
            }
        }
    }
}

async fn flush(pool: &PgPool, buf: &mut Vec<ChatMessage>) {
    let n = buf.len();

    let ids: Vec<i64>      = buf.iter().map(|m| m.id as i64).collect();
    let rooms: Vec<String> = buf.iter().map(|m| m.room_id.clone()).collect();
    let senders: Vec<String> = buf.iter().map(|m| m.sender_id.clone()).collect();
    let payloads: Vec<Vec<u8>> = buf.iter().map(|m| m.payload.to_vec()).collect();
    let timestamps: Vec<i64> = buf.iter().map(|m| m.timestamp).collect();

    let result = sqlx::query(
        "INSERT INTO messages (id, room_id, sender_id, payload, timestamp)
         SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::text[], $4::bytea[], $5::bigint[])
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(ids)
    .bind(rooms)
    .bind(senders)
    .bind(payloads)
    .bind(timestamps)
    .execute(pool)
    .await;

    match result {
        Ok(r) => info!("flushed {n} msgs → postgres ({} inserted)", r.rows_affected()),
        Err(e) => error!("bulk insert failed: {e}"),
    }

    buf.clear();
}
