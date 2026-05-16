/// Stress test binary for Turbo Chat Engine.
/// Usage: stress <clients> <msgs_per_client> <room_size> [--sync]
///
/// Default mode: ramp-up — measures sustained throughput over time
/// --sync mode:  all clients connect → barrier → fire simultaneously → measure peak msg/s
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio::net::TcpStream;
use tokio::sync::{Barrier, Semaphore};
use tokio_tungstenite::{client_async, tungstenite::Message};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/turbo_chat.rs"));
}
use proto::{envelope::Kind, ChatMessage, Envelope};

const SOURCE_IPS: &[&str]    = &["127.0.0.1", "127.0.0.2"];
const SERVER_ADDR: &str      = "127.0.0.1:8080";
const SERVER_URL: &str       = "ws://127.0.0.1:8080";
const CONNECT_CONCURRENCY: usize = 500;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let sync_mode = args.iter().any(|a| a == "--sync");

    let num_clients: usize    = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1_000);
    let msgs_per_client: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let room_size: usize      = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);

    let num_rooms = (num_clients + room_size - 1) / room_size;

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Turbo Chat Engine  —  Rust Stress Test");
    println!("  Mode: {}", if sync_mode { "SYNCHRONIZED BURST" } else { "RAMP-UP" });
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Clients       : {num_clients}");
    println!("  Room size     : {room_size}");
    println!("  Rooms         : {num_rooms}");
    println!("  Msgs/client   : {msgs_per_client}");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    if sync_mode {
        run_sync(num_clients, msgs_per_client, room_size).await
    } else {
        run_rampup(num_clients, msgs_per_client, room_size).await
    }
}

// ── Synchronized burst ────────────────────────────────────────────────────────
//
// Phase 1 — connect:   all tasks connect + join their room, then DROP the
//                      semaphore permit so other tasks can run.
// Phase 2 — barrier:   every task (including main) calls barrier.wait().
//                      All block until the last one arrives.
// Phase 3 — fire:      all send simultaneously; we measure the real burst window.

async fn run_sync(num_clients: usize, msgs_per_client: usize, room_size: usize) -> Result<()> {
    // barrier size = num_clients tasks + 1 main
    let barrier      = Arc::new(Barrier::new(num_clients + 1));
    let received     = Arc::new(AtomicU64::new(0));
    let connected    = Arc::new(AtomicU64::new(0));
    let failed       = Arc::new(AtomicU64::new(0));
    let sem          = Arc::new(Semaphore::new(CONNECT_CONCURRENCY));
    // u64::MAX so first CAS always wins
    let t_first_send = Arc::new(AtomicU64::new(u64::MAX));
    let t_last_recv  = Arc::new(AtomicU64::new(0));

    // each message fans out to room_size receivers (including sender)
    let expected: u64 = (num_clients * msgs_per_client * room_size) as u64;

    let connect_start = Instant::now();
    let mut handles   = Vec::with_capacity(num_clients);

    for i in 0..num_clients {
        let room_id   = format!("room-{}", i / room_size);
        let sender_id = format!("c{i}");
        let src_ip    = SOURCE_IPS[i % SOURCE_IPS.len()];

        let join_bytes = encode(i as u64, &room_id, &sender_id, b"join");
        let msg_bytes: Vec<Vec<u8>> = (0..msgs_per_client)
            .map(|j| encode((i * 1000 + j) as u64, &room_id, &sender_id, b"hi"))
            .collect();

        handles.push(tokio::spawn({
            let barrier      = barrier.clone();
            let received     = received.clone();
            let connected    = connected.clone();
            let failed       = failed.clone();
            let sem          = sem.clone();
            let t_first_send = t_first_send.clone();
            let t_last_recv  = t_last_recv.clone();
            let src_ip       = src_ip.to_string();

            async move {
                // ── Phase 1: connect ──────────────────────────────────────
                let permit = sem.acquire().await.unwrap();

                let socket = match tcp_connect_from(&src_ip, SERVER_ADDR).await {
                    Ok(s)  => s,
                    Err(_) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        drop(permit);           // release BEFORE barrier
                        barrier.wait().await;
                        return;
                    }
                };

                let Ok((ws, _)) = client_async(SERVER_URL, socket).await else {
                    failed.fetch_add(1, Ordering::Relaxed);
                    drop(permit);
                    barrier.wait().await;
                    return;
                };

                let (mut tx, mut rx) = ws.split();
                let _ = tx.send(Message::Binary(join_bytes)).await;
                connected.fetch_add(1, Ordering::Relaxed);

                // KEY: release the semaphore permit so the next task can connect.
                // Without this, only 500 tasks reach the barrier and it deadlocks.
                drop(permit);

                // ── Phase 2: wait for everyone ────────────────────────────
                barrier.wait().await;

                // ── Phase 3: fire simultaneously ──────────────────────────
                let send_ns = now_ns();
                // record earliest send time (CAS loop — no fetch_min on stable)
                let mut cur = t_first_send.load(Ordering::Relaxed);
                while send_ns < cur {
                    match t_first_send.compare_exchange_weak(
                        cur, send_ns, Ordering::Relaxed, Ordering::Relaxed,
                    ) {
                        Ok(_)  => break,
                        Err(v) => cur = v,
                    }
                }

                for bytes in msg_bytes {
                    let _ = tx.send(Message::Binary(bytes)).await;
                }

                // count deliveries; exit when global total reached OR after 5s quiet
                let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
                loop {
                    match tokio::time::timeout_at(deadline, rx.next()).await {
                        Ok(Some(Ok(Message::Binary(_)))) => {
                            let n = received.fetch_add(1, Ordering::Relaxed) + 1;
                            t_last_recv.store(now_ns(), Ordering::Relaxed);
                            if n >= expected { break; }
                        }
                        _ => break, // timeout, close frame, or error — we're done
                    }
                }

                let _ = tx.close().await;
            }
        }));
    }

    // Progress printer while clients are connecting
    println!("  → phase 1: connecting {} clients (concurrency={})", num_clients, CONNECT_CONCURRENCY);
    loop {
        let c = connected.load(Ordering::Relaxed);
        let f = failed.load(Ordering::Relaxed);
        if c + f >= num_clients as u64 { break; }
        if (c + f) % 10_000 == 0 && c + f > 0 {
            println!("    {}/{} done (ok={c} fail={f})", c + f, num_clients);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    let connect_ms = connect_start.elapsed().as_millis();
    let c = connected.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);
    println!("  → phase 1 done in {connect_ms}ms — ok={c} fail={f}");
    println!("  → phase 2: barrier — releasing burst now!\n");

    // Main participates in the barrier — this unblocks all waiting tasks
    barrier.wait().await;

    // Wait for all delivery tasks to finish
    for h in handles { let _ = h.await; }

    let t0 = t_first_send.load(Ordering::Relaxed);
    let t1 = t_last_recv.load(Ordering::Relaxed);
    let total_rcv = received.load(Ordering::Relaxed);

    let burst_ms  = if t1 > t0 { (t1 - t0) as f64 / 1_000_000.0 } else { 1.0 };
    let peak_tput = total_rcv as f64 / (burst_ms / 1000.0);
    let avg_us    = if total_rcv > 0 { burst_ms * 1000.0 / total_rcv as f64 } else { 0.0 };

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  SYNCHRONIZED BURST RESULTS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Connection phase  {connect_ms} ms");
    println!("  Connected         {c}/{num_clients}  (failed={f})");
    println!("  ─────────────────────────────────────────");
    println!("  Burst window      {burst_ms:.1} ms");
    println!("  Msgs delivered    {total_rcv} / {expected}");
    println!("  Peak throughput   {peak_tput:.0} msg/s");
    println!("  Avg msg latency   {avg_us:.1} µs");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    Ok(())
}

// ── Ramp-up mode (original) ───────────────────────────────────────────────────

async fn run_rampup(num_clients: usize, msgs_per_client: usize, room_size: usize) -> Result<()> {
    let received  = Arc::new(AtomicU64::new(0));
    let connected = Arc::new(AtomicU64::new(0));
    let failed    = Arc::new(AtomicU64::new(0));
    let sem       = Arc::new(Semaphore::new(CONNECT_CONCURRENCY));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(num_clients);

    for i in 0..num_clients {
        let room_id   = format!("room-{}", i / room_size);
        let sender_id = format!("c{i}");
        let src_ip    = SOURCE_IPS[i % SOURCE_IPS.len()];

        let join_bytes = encode(i as u64, &room_id, &sender_id, b"join");
        let msg_bytes: Vec<Vec<u8>> = (0..msgs_per_client)
            .map(|j| encode((i * 1000 + j) as u64, &room_id, &sender_id, b"hi"))
            .collect();

        handles.push(tokio::spawn({
            let received  = received.clone();
            let connected = connected.clone();
            let failed    = failed.clone();
            let sem       = sem.clone();
            let src_ip    = src_ip.to_string();

            async move {
                let _permit = sem.acquire().await.unwrap();

                let socket = match tcp_connect_from(&src_ip, SERVER_ADDR).await {
                    Ok(s)  => s,
                    Err(_) => { failed.fetch_add(1, Ordering::Relaxed); return; }
                };

                let Ok((ws, _)) = client_async(SERVER_URL, socket).await else {
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                connected.fetch_add(1, Ordering::Relaxed);

                let (mut tx, mut rx) = ws.split();
                let recv_cnt = received.clone();
                let reader = tokio::spawn(async move {
                    while let Some(Ok(Message::Binary(_))) = rx.next().await {
                        recv_cnt.fetch_add(1, Ordering::Relaxed);
                    }
                });

                let _ = tx.send(Message::Binary(join_bytes)).await;
                for bytes in msg_bytes {
                    let _ = tx.send(Message::Binary(bytes)).await;
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
                let _ = tx.close().await;
                reader.abort();
            }
        }));

        if i % 1_000 == 999 {
            let c = connected.load(Ordering::Relaxed);
            let f = failed.load(Ordering::Relaxed);
            println!("  → {}/{} spawned (connected={c}, failed={f})", i + 1, num_clients);
        }
    }

    println!("  → all tasks launched, waiting...\n");
    for h in handles { let _ = h.await; }

    let elapsed   = start.elapsed().as_secs_f64();
    let total_rcv = received.load(Ordering::Relaxed);
    let c         = connected.load(Ordering::Relaxed);
    let f         = failed.load(Ordering::Relaxed);

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  RESULTS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Connected        {c}/{num_clients}  (failed={f})");
    println!("  Elapsed          {elapsed:.2}s");
    println!("  Msgs delivered   {total_rcv}");
    println!("  Throughput       {:.0} msg/s", total_rcv as f64 / elapsed);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

async fn tcp_connect_from(src_ip: &str, dst: &str) -> Result<TcpStream> {
    let src: SocketAddr = format!("{src_ip}:0").parse()?;
    let dst: SocketAddr = dst.parse()?;
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.bind(src)?;
    Ok(socket.connect(dst).await?)
}

fn encode(id: u64, room: &str, sender: &str, payload: &[u8]) -> Vec<u8> {
    let env = Envelope {
        kind: Some(Kind::Message(ChatMessage {
            id,
            room_id:   room.to_string(),
            sender_id: sender.to_string(),
            payload:   payload.to_vec(),
            timestamp: 0,
        })),
    };
    let mut buf = Vec::with_capacity(env.encoded_len());
    env.encode(&mut buf).unwrap();
    buf
}
