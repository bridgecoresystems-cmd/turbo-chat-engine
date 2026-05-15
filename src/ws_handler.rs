use bytes::Bytes;
use fastwebsockets::{upgrade::UpgradeFut, FragmentCollector, Frame, OpCode, Payload};
use prost::Message as ProstMessage;
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use crate::{
    proto::{envelope::Kind, Envelope},
    state::AppState,
};

pub async fn handle_ws(fut: UpgradeFut, state: AppState) {
    let mut ws = match fut.await {
        Ok(ws) => FragmentCollector::new(ws),
        Err(e) => {
            error!("ws upgrade failed: {e}");
            return;
        }
    };

    // First binary frame must be an Envelope{ChatMessage} — declares room + sender
    let first_frame = match ws.read_frame().await {
        Ok(f) if f.opcode == OpCode::Binary => f,
        Ok(_) => {
            warn!("first frame must be binary protobuf");
            return;
        }
        Err(e) => {
            error!("read error on first frame: {e}");
            return;
        }
    };

    let envelope = match Envelope::decode(first_frame.payload.as_ref()) {
        Ok(e) => e,
        Err(e) => {
            error!("protobuf decode error: {e}");
            return;
        }
    };

    let chat_msg = match envelope.kind {
        Some(Kind::Message(m)) => m,
        _ => {
            warn!("first envelope must contain a ChatMessage");
            return;
        }
    };

    let room_id = chat_msg.room_id.clone();
    let sender_id = chat_msg.sender_id.clone();
    info!("{sender_id} joined room '{room_id}'");

    let mut rx = state.join_room(&room_id).await;

    // Publish the join message itself so everyone in the room sees it
    let first_raw = Bytes::copy_from_slice(first_frame.payload.as_ref());
    state.publish(&room_id, first_raw).await;

    loop {
        tokio::select! {
            // Broadcast from other clients in the same room
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

            // Message from this client
            read = ws.read_frame() => match read {
                Ok(frame) => match frame.opcode {
                    OpCode::Close => break,
                    OpCode::Binary => {
                        let data = Bytes::copy_from_slice(frame.payload.as_ref());
                        state.publish(&room_id, data).await;
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
