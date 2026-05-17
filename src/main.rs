use anyhow::Result;
use bytes::Bytes;
use fastwebsockets::upgrade;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    header, server::conn::http1, service::service_fn, Method, Request, Response,
};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, net::SocketAddr};
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{error, info, warn};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/turbo_chat.rs"));
}

mod auth;
mod persistence;
mod state;
mod ws_handler;

use state::AppState;

type BoxedBody = BoxBody<Bytes, Infallible>;

fn body_empty() -> BoxedBody {
    Empty::<Bytes>::new().map_err(|never| match never {}).boxed()
}

fn body_json(s: &str) -> BoxedBody {
    Full::new(Bytes::from(s.to_string()))
        .map_err(|never| match never {})
        .boxed()
}

fn json_response(status: u16, body: &str) -> Result<Response<BoxedBody>> {
    Ok(Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(body_json(body))
        .unwrap())
}

async fn handle_http(
    mut req: Request<hyper::body::Incoming>,
    state: AppState,
) -> Result<Response<BoxedBody>, Infallible> {
    let path   = req.uri().path().to_string();
    let method = req.method().clone();

    // ── GET /health ───────────────────────────────────────────────────────────
    if method == Method::GET && path == "/health" {
        return Ok(json_response(200, r#"{"status":"ok"}"#).unwrap());
    }

    // ── GET /history/:room_id?limit=50 ────────────────────────────────────────
    if method == Method::GET && path.starts_with("/history/") {
        let room_id = path.trim_start_matches("/history/").to_string();
        if room_id.is_empty() {
            return Ok(json_response(400, r#"{"error":"missing room_id"}"#).unwrap());
        }
        let limit: i64 = req
            .uri()
            .query()
            .and_then(|q| q.split('&').find_map(|p| p.strip_prefix("limit=")))
            .and_then(|v| v.parse().ok())
            .unwrap_or(50)
            .min(200);

        return match state.get_history(&room_id, limit).await {
            Ok(msgs) => {
                let json = serde_json::to_string(&msgs).unwrap_or_else(|_| "[]".into());
                Ok(json_response(200, &json).unwrap())
            }
            Err(e) => {
                error!("history query: {e}");
                Ok(json_response(500, r#"{"error":"internal error"}"#).unwrap())
            }
        };
    }

    // ── WebSocket upgrade ─────────────────────────────────────────────────────
    if !upgrade::is_upgrade_request(&req) {
        return Ok(json_response(400, r#"{"error":"expected websocket upgrade"}"#).unwrap());
    }

    let token = auth::token_from_query(req.uri().query());
    let claims = match token.as_deref().map(|t| auth::verify(t, &state.jwt_secret)) {
        Some(Ok(c))  => c,
        Some(Err(e)) => {
            warn!("rejected: {e}");
            return Ok(Response::builder().status(401).body(body_empty()).unwrap());
        }
        None => {
            warn!("rejected: missing token");
            return Ok(Response::builder().status(401).body(body_empty()).unwrap());
        }
    };

    let (res, fut) = match upgrade::upgrade(&mut req) {
        Ok(r)  => r,
        Err(e) => {
            error!("upgrade error: {e}");
            return Ok(Response::builder().status(500).body(body_empty()).unwrap());
        }
    };
    tokio::spawn(async move {
        ws_handler::handle_ws(fut, state, claims).await;
    });

    Ok(res.map(|_| body_empty()))
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
    let worker = persistence::BatchWorker::new(persist_rx, pool.clone());
    tokio::spawn(async move { worker.run().await });
    info!("batch worker started (flush every 2s or 1000 msgs)");

    // ── Redis ─────────────────────────────────────────────────────────────────
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".to_string());

    // ── JWT ───────────────────────────────────────────────────────────────────
    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "dev-secret-change-in-production".to_string());
    info!("JWT auth enabled");

    let state = AppState::new(&redis_url, persist_tx, jwt_secret, pool).await?;
    state.start_redis_dispatcher();
    info!("connected to Redis at {redis_url}");

    // ── HTTP + WebSocket server ───────────────────────────────────────────────
    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let io    = TokioIo::new(stream);
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
