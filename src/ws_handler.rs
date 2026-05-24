use bytes::Bytes;
use fastwebsockets::{upgrade::UpgradeFut, FragmentCollector, Frame, OpCode, Payload};
use prost::Message as ProstMessage;
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use crate::{
    auth::Claims,
    proto::{envelope::Kind, Ack, Envelope, Presence},
    rate_limit::{RateLimiter, WARN_INTERVAL},
    state::{AppState, RATE_LIMIT_MSG_PER_SEC},
};

pub async fn handle_ws(fut: UpgradeFut, state: AppState, claims: Claims) {
    let mut ws = match fut.await {
        Ok(ws) => FragmentCollector::new(ws),
        Err(e) => {
            error!("ws upgrade failed: {e}");
            return;
        }
    };

    // First frame: client declares which room to join
    let first_frame = match ws.read_frame().await {
        Ok(f) if f.opcode == OpCode::Binary => f,
        Ok(_) => { warn!("{} sent non-binary first frame", claims.sub); return; }
        Err(e) => { error!("first frame error from {}: {e}", claims.sub); return; }
    };

    let envelope = match Envelope::decode(first_frame.payload.as_ref()) {
        Ok(e) => e,
        Err(e) => { error!("proto decode error from {}: {e}", claims.sub); return; }
    };

    let mut chat_msg = match envelope.kind {
        Some(Kind::Message(m)) => m,
        _ => { warn!("{}: first frame must be ChatMessage", claims.sub); return; }
    };

    let room_id   = chat_msg.room_id.clone();
    let sender_id = claims.sub.clone(); // trusted from JWT
    chat_msg.sender_id = sender_id.clone();

    info!("{sender_id} (role={}) joined room '{room_id}'", claims.role);

    let mut rx       = state.join_room(&room_id).await;
    let mut limiter  = RateLimiter::new(RATE_LIMIT_MSG_PER_SEC);
    let mut last_warn = std::time::Instant::now() - WARN_INTERVAL;

    // Broadcast presence: online
    broadcast_presence(&state, &room_id, &sender_id, "online").await;

    // Persist and broadcast join message
    let join_bytes = encode_envelope(Kind::Message(chat_msg.clone()));
    state.record(chat_msg).await;
    state.publish(&room_id, join_bytes).await;

    loop {
        tokio::select! {
            // Incoming from Redis broadcast → send to this client
            recv = rx.recv() => match recv {
                Ok(data) => {
                    let frame = Frame::binary(Payload::Owned(data.to_vec()));
                    if let Err(e) = ws.write_frame(frame).await {
                        error!("{sender_id} write error: {e}");
                        break;
                    }
                }
                Err(RecvError::Lagged(n)) => warn!("{sender_id} lagged by {n} messages"),
                Err(RecvError::Closed)    => break,
            },

            // Incoming from this client → route to room
            read = ws.read_frame() => match read {
                Ok(frame) => match frame.opcode {
                    OpCode::Close => break,
                    OpCode::Binary => {
                        // Rate limit check
                        if !limiter.allow() {
                            if last_warn.elapsed() >= WARN_INTERVAL {
                                warn!("{sender_id} rate-limited (>{RATE_LIMIT_MSG_PER_SEC} msg/s)");
                                last_warn = std::time::Instant::now();
                            }
                            continue; // drop message silently
                        }

                        let raw = Bytes::copy_from_slice(frame.payload.as_ref());

                        if let Ok(env) = Envelope::decode(raw.as_ref()) {
                            match env.kind {
                                Some(Kind::Message(mut msg)) => {
                                    msg.sender_id = sender_id.clone();
                                    state.record(msg).await;
                                    state.publish(&room_id, raw).await;
                                }
                                Some(Kind::Typing(mut t)) => {
                                    t.user_id = sender_id.clone();
                                    let bytes = encode_envelope(Kind::Typing(t));
                                    state.publish(&room_id, bytes).await;
                                }
                                Some(Kind::Ack(ack)) => {
                                    state.record_read(ack.message_id, &sender_id, &room_id).await;
                                    let ack_out = Ack {
                                        message_id: ack.message_id,
                                        user_id: sender_id.clone(),
                                        room_id: room_id.clone(),
                                    };
                                    let bytes = encode_envelope(Kind::Ack(ack_out));
                                    state.publish(&room_id, bytes).await;
                                }
                                _ => {
                                    state.publish(&room_id, raw).await;
                                }
                            }
                        }
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

    // Broadcast presence: offline
    broadcast_presence(&state, &room_id, &sender_id, "offline").await;
    info!("{sender_id} left room '{room_id}'");
}

async fn broadcast_presence(state: &AppState, room_id: &str, user_id: &str, status: &str) {
    let bytes = encode_envelope(Kind::Presence(Presence {
        room_id:  room_id.to_string(),
        user_id:  user_id.to_string(),
        status:   status.to_string(),
    }));
    state.publish(room_id, bytes).await;
}

fn encode_envelope(kind: Kind) -> Bytes {
    let env = Envelope { kind: Some(kind) };
    let mut buf = Vec::with_capacity(env.encoded_len());
    env.encode(&mut buf).unwrap();
    Bytes::from(buf)
}
