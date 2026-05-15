/// Stress test binary for Turbo Chat Engine.
/// Usage: stress <clients> <msgs_per_client> <room_size>
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
use tokio::sync::Semaphore;
use tokio_tungstenite::{client_async, tungstenite::Message};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/turbo_chat.rs"));
}
use proto::{envelope::Kind, ChatMessage, Envelope};

// Two source IPs for 100k+ connections (each gives ~64k ports)
const SOURCE_IPS: &[&str] = &["127.0.0.1", "127.0.0.2"];
const SERVER_ADDR: &str   = "127.0.0.1:8080";
const SERVER_URL: &str    = "ws://127.0.0.1:8080";
const CONNECT_CONCURRENCY: usize = 500;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let num_clients: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1_000);
    let msgs_per_client: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    let room_size: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);

    let num_rooms = (num_clients + room_size - 1) / room_size;
    let half = num_clients / SOURCE_IPS.len();

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Turbo Chat Engine  —  Rust Stress Test");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Clients       : {num_clients}");
    println!("  Room size     : {room_size} клиентов/комната");
    println!("  Rooms         : {num_rooms}");
    println!("  Msgs/client   : {msgs_per_client}");
    println!("  Source IPs    : {} ({} клиентов каждый)", SOURCE_IPS.join(", "), half);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let received  = Arc::new(AtomicU64::new(0));
    let connected = Arc::new(AtomicU64::new(0));
    let failed    = Arc::new(AtomicU64::new(0));
    let sem       = Arc::new(Semaphore::new(CONNECT_CONCURRENCY));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(num_clients);

    for i in 0..num_clients {
        let room_id   = format!("room-{}", i / room_size);
        let sender_id = format!("c{i}");

        // чередуем source IP чтобы обойти лимит портов
        let src_ip = SOURCE_IPS[i % SOURCE_IPS.len()];

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

                // Bind к конкретному source IP и подключаемся к серверу
                let socket = match tcp_connect_from(&src_ip, SERVER_ADDR).await {
                    Ok(s) => s,
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
            let conn = connected.load(Ordering::Relaxed);
            let fail = failed.load(Ordering::Relaxed);
            println!("  → {}/{} spawned  (connected={conn}, failed={fail})", i + 1, num_clients);
        }
    }

    println!("  → все таски запущены, ждём...\n");
    for h in handles { let _ = h.await; }

    let elapsed    = start.elapsed().as_secs_f64();
    let total_rcv  = received.load(Ordering::Relaxed);
    let total_conn = connected.load(Ordering::Relaxed);
    let total_fail = failed.load(Ordering::Relaxed);

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  РЕЗУЛЬТАТЫ");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Connected        {total_conn}/{num_clients}");
    println!("  Failed           {total_fail}");
    println!("  Elapsed          {elapsed:.2}s");
    println!("  Msgs delivered   {total_rcv}");
    println!("  Throughput       {:.0} msg/s", total_rcv as f64 / elapsed);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    Ok(())
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
