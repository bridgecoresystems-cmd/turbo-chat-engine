use bytes::Bytes;
use fastwebsockets::{upgrade::UpgradeFut, FragmentCollector, Frame, OpCode, Payload};
use prost::Message as ProstMessage;
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use crate::{
    auth::Claims,
    proto::{envelope::Kind, Envelope},
    state::AppState,
};

pub async fn handle_ws(fut: UpgradeFut, state: AppState, claims: Claims) {
    let mut ws = match fut.await {
        Ok(ws) => FragmentCollector::new(ws),
        Err(e) => {
            error!("ws upgrade failed: {e}");
            return;
        }
    };

    // First binary frame declares which room to join.
    // sender_id is taken from JWT claims — client cannot spoof it.
    let first_frame = match ws.read_frame().await {
        Ok(f) if f.opcode == OpCode::Binary => f,
        Ok(_) => {
            warn!("{} sent non-binary first frame", claims.sub);
            return;
        }
        Err(e) => {
            error!("read error on first frame from {}: {e}", claims.sub);
            return;
        }
    };

    let envelope = match Envelope::decode(first_frame.payload.as_ref()) {
        Ok(e) => e,
        Err(e) => {
            error!("protobuf decode error from {}: {e}", claims.sub);
            return;
        }
    };

    let mut chat_msg = match envelope.kind {
        Some(Kind::Message(m)) => m,
        _ => {
            warn!("{}: first envelope must contain a ChatMessage", claims.sub);
            return;
        }
    };

    let room_id  = chat_msg.room_id.clone();
    let sender_id = claims.sub.clone(); // trusted identity from JWT

    // Overwrite whatever the client sent — sender_id is authoritative from the token
    chat_msg.sender_id = sender_id.clone();

    info!("{sender_id} (role={}) joined room '{room_id}'", claims.role);

    let mut rx = state.join_room(&room_id).await;

    // Persist and broadcast the join message with the trusted sender_id
    let join_bytes = {
        let env = Envelope { kind: Some(Kind::Message(chat_msg.clone())) };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).unwrap();
        Bytes::from(buf)
    };
    state.record(chat_msg).await;
    state.publish(&room_id, join_bytes).await;

    loop {
        tokio::select! {
            recv = rx.recv() => match recv {
                Ok(data) => {
                    let frame = Frame::binary(Payload::Owned(data.to_vec()));
                    if let Err(e) = ws.write_frame(frame).await {
                        error!("{sender_id} write error: {e}");
                        break;
                    }
                }
                Err(RecvError::Lagged(n)) => warn!("{sender_id} lagged by {n} messages"),
                Err(RecvError::Closed) => break,
            },

            read = ws.read_frame() => match read {
                Ok(frame) => match frame.opcode {
                    OpCode::Close => break,
                    OpCode::Binary => {
                        let raw = Bytes::copy_from_slice(frame.payload.as_ref());

                        if let Ok(env) = Envelope::decode(raw.as_ref()) {
                            if let Some(Kind::Message(mut msg)) = env.kind {
                                msg.sender_id = sender_id.clone(); // enforce trusted id
                                state.record(msg).await;
                            }
                        }

                        state.publish(&room_id, raw).await;
                    }
                    _ => {}
                },
                Err(e) => {
                    error!("{sender_id} read error: {e}");
                    break;
                }
            },
        }
    }

    info!("{sender_id} left room '{room_id}'");
}
