use anyhow::Result;
use fastwebsockets::upgrade;
use http_body_util::Empty;
use hyper::{body::Bytes, server::conn::http1, service::service_fn, Request, Response};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{error, info};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/turbo_chat.rs"));
}

mod persistence;
mod state;
mod ws_handler;

use state::AppState;

async fn handle_http(
    mut req: Request<hyper::body::Incoming>,
    state: AppState,
) -> Result<Response<Empty<Bytes>>, anyhow::Error> {
    if upgrade::is_upgrade_request(&req) {
        let (res, fut) = upgrade::upgrade(&mut req)?;
        tokio::spawn(async move {
            ws_handler::handle_ws(fut, state).await;
        });
        Ok(res)
    } else {
        Ok(Response::builder().status(400).body(Empty::new()).unwrap())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("turbo_chat_engine=info".parse()?),
        )
        .init();

    // ── PostgreSQL ────────────────────────────────────────────────────────────
    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://bridgecore_admin:super_secret_password@127.0.0.1:5433/turbo_chat_db"
            .to_string()
    });
    let pool = sqlx::PgPool::connect(&db_url).await?;
    info!("connected to PostgreSQL");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS messages (
            id         BIGINT PRIMARY KEY,
            room_id    TEXT   NOT NULL,
            sender_id  TEXT   NOT NULL,
            payload    BYTEA  NOT NULL,
            timestamp  BIGINT NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    let (persist_tx, persist_rx) = mpsc::channel(8_192);
    let worker = persistence::BatchWorker::new(persist_rx, pool);
    tokio::spawn(async move { worker.run().await });
    info!("batch worker started (flush every 2s or 1000 msgs)");

    // ── Redis ─────────────────────────────────────────────────────────────────
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".to_string());
    let state = AppState::new(&redis_url, persist_tx).await?;
    info!("connected to Redis at {redis_url}");

    // ── WebSocket server ──────────────────────────────────────────────────────
    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        info!("new connection from {peer}");
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let svc = service_fn(move |req| handle_http(req, state.clone()));
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, svc)
                .with_upgrades()
                .await
            {
                error!("connection error from {peer}: {e}");
            }
        });
    }
}
