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
mod rate_limit;
mod state;
mod storage;
mod ws_handler;

use state::AppState;
use storage::R2Storage;

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

fn query_param<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query?.split('&').find_map(|p| p.strip_prefix(key)?.strip_prefix('='))
}

async fn handle_http(
    mut req: Request<hyper::body::Incoming>,
    state: AppState,
) -> Result<Response<BoxedBody>, Infallible> {
    let path   = req.uri().path().to_string();
    let method = req.method().clone();
    let query  = req.uri().query().map(str::to_string);

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
        let limit: i64 = query_param(query.as_deref(), "limit")
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

    // ── GET /upload-url?filename=photo.jpg&content_type=image%2Fjpeg ─────────
    if method == Method::GET && path == "/upload-url" {
        // Требуем JWT
        let token = auth::token_from_query(query.as_deref());
        match token.as_deref().map(|t| auth::verify(t, &state.jwt_secret)) {
            Some(Ok(_))  => {}
            Some(Err(e)) => {
                warn!("upload-url rejected: {e}");
                return Ok(Response::builder().status(401).body(body_empty()).unwrap());
            }
            None => {
                warn!("upload-url rejected: missing token");
                return Ok(Response::builder().status(401).body(body_empty()).unwrap());
            }
        }

        let filename = match query_param(query.as_deref(), "filename") {
            Some(f) if !f.is_empty() => f,
            _ => return Ok(json_response(400, r#"{"error":"missing filename"}"#).unwrap()),
        };
        let content_type = query_param(query.as_deref(), "content_type")
            .unwrap_or("application/octet-stream");

        let ext = filename.rsplit('.').next().unwrap_or("bin");
        let key = format!("{}/{}.{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4(), ext);

        return match state.storage.presigned_put(&key, content_type).await {
            Ok(upload_url) => {
                let file_url = state.storage.public_url(&key);
                let body = serde_json::json!({
                    "upload_url": upload_url,
                    "file_url":   file_url,
                    "key":        key,
                });
                Ok(json_response(200, &body.to_string()).unwrap())
            }
            Err(e) => {
                error!("presign error: {e}");
                Ok(json_response(500, r#"{"error":"storage error"}"#).unwrap())
            }
        };
    }

    // ── WebSocket upgrade ─────────────────────────────────────────────────────
    if !upgrade::is_upgrade_request(&req) {
        return Ok(json_response(400, r#"{"error":"expected websocket upgrade"}"#).unwrap());
    }

    let token = auth::token_from_query(query.as_deref());
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

    // ── Cloudflare R2 ─────────────────────────────────────────────────────────
    let r2_account_id  = std::env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID required");
    let r2_access_key  = std::env::var("R2_ACCESS_KEY_ID").expect("R2_ACCESS_KEY_ID required");
    let r2_secret_key  = std::env::var("R2_SECRET_ACCESS_KEY").expect("R2_SECRET_ACCESS_KEY required");
    let r2_bucket      = std::env::var("R2_BUCKET").unwrap_or_else(|_| "turbo-chat-files".to_string());
    let r2_public_url  = std::env::var("R2_PUBLIC_URL")
        .unwrap_or_else(|_| format!("https://{r2_account_id}.r2.cloudflarestorage.com/{r2_bucket}"));
    let storage = R2Storage::new(&r2_account_id, &r2_access_key, &r2_secret_key, &r2_bucket, &r2_public_url);
    info!("R2 storage ready (bucket: {r2_bucket})");

    let state = AppState::new(&redis_url, persist_tx, jwt_secret, pool, storage).await?;
    state.start_redis_dispatcher();
    info!("connected to Redis at {redis_url}");

    // ── HTTP + WebSocket server ───────────────────────────────────────────────
    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("listening on {addr}");

    // Graceful shutdown: Ctrl+C или SIGTERM
    let shutdown = async {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = async {
                #[cfg(unix)]
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler failed")
                    .recv()
                    .await;
                #[cfg(not(unix))]
                std::future::pending::<()>().await;
            } => {},
        }
    };
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            Ok((stream, peer)) = listener.accept() => {
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
            _ = &mut shutdown => {
                info!("shutdown signal received — stopping");
                break;
            }
        }
    }

    info!("server stopped");
    Ok(())
}
