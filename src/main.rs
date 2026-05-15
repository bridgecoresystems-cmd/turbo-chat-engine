use anyhow::Result;
use fastwebsockets::{upgrade, FragmentCollector, OpCode, WebSocketError};
use http_body_util::Empty;
use hyper::{body::Bytes, server::conn::http1, service::service_fn, Request, Response};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/turbo_chat.rs"));
}

async fn handle_ws(fut: upgrade::UpgradeFut) -> Result<(), WebSocketError> {
    let ws = FragmentCollector::new(fut.await?);
    let mut ws = ws;

    info!("client connected");

    loop {
        let frame = ws.read_frame().await?;
        match frame.opcode {
            OpCode::Close => break,
            OpCode::Binary => {
                // echo back — will be replaced by Redis routing
                ws.write_frame(fastwebsockets::Frame::binary(frame.payload))
                    .await?;
            }
            _ => {}
        }
    }

    info!("client disconnected");
    Ok(())
}

async fn handle_http(
    mut req: Request<hyper::body::Incoming>,
) -> Result<Response<Empty<Bytes>>, anyhow::Error> {
    if upgrade::is_upgrade_request(&req) {
        let (res, fut) = upgrade::upgrade(&mut req)?;
        tokio::spawn(async move {
            if let Err(e) = handle_ws(fut).await {
                error!("ws error: {e}");
            }
        });
        Ok(res)
    } else {
        Ok(Response::builder()
            .status(400)
            .body(Empty::new())
            .unwrap())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("turbo_chat_engine=info".parse()?),
        )
        .init();

    let addr: SocketAddr = "0.0.0.0:8080".parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        info!("new connection from {peer}");
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service_fn(handle_http))
                .with_upgrades()
                .await
            {
                error!("connection error from {peer}: {e}");
            }
        });
    }
}
