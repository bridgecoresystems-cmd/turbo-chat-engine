import WebSocket from "ws";
import protobuf from "protobufjs";
import path from "path";

const PROTO_PATH = path.resolve(__dirname, "../proto/message.proto");
const SERVER_URL  = "ws://localhost:8080";

// ── Параметры теста ────────────────────────────────────────────────────────
const NUM_CLIENTS       = parseInt(process.env.CLIENTS  ?? "500");
const MSGS_PER_CLIENT   = parseInt(process.env.MSGS     ?? "20");
const ROOM              = "stress-room";
// ──────────────────────────────────────────────────────────────────────────

const root     = await protobuf.load(PROTO_PATH);
const Envelope = root.lookupType("turbo_chat.Envelope");

let msgId = 0;
function encode(senderId: string, text: string): Buffer {
  const env = Envelope.create({
    kind: "message",
    message: {
      id:        ++msgId,
      roomId:    ROOM,
      senderId,
      payload:   Buffer.from(text, "utf8"),
      timestamp: Date.now(),
    },
  });
  return Buffer.from(Envelope.encode(env).finish());
}

// ── Подключаем одного клиента ──────────────────────────────────────────────
function spawnClient(id: number): Promise<{ sent: number; received: number; latencyMs: number[] }> {
  return new Promise((resolve, reject) => {
    const ws      = new WebSocket(SERVER_URL);
    const name    = `client_${id}`;
    let sent      = 0;
    let received  = 0;
    const latencies: number[] = [];
    const pending = new Map<number, number>(); // msgId → sendTime

    ws.on("error", reject);

    ws.on("open", () => {
      // join
      ws.send(encode(name, "join"));
      sent++;

      // отправляем MSGS_PER_CLIENT сообщений с небольшим интервалом
      let i = 0;
      const interval = setInterval(() => {
        if (i >= MSGS_PER_CLIENT) { clearInterval(interval); return; }
        const currentId = msgId + 1;
        const t = Date.now();
        ws.send(encode(name, `msg-${i}`));
        pending.set(currentId, t);
        sent++;
        i++;
      }, 5);
    });

    ws.on("message", (data: Buffer) => {
      received++;
      try {
        const env = Envelope.decode(data) as any;
        if (env.message) {
          const t = pending.get(env.message.id);
          if (t) { latencies.push(Date.now() - t); pending.delete(env.message.id); }
        }
      } catch {}

      // закрываем когда получили достаточно
      if (received >= NUM_CLIENTS * (MSGS_PER_CLIENT + 1)) {
        ws.close();
        resolve({ sent, received, latencyMs: latencies });
      }
    });

    ws.on("close", () => resolve({ sent, received, latencyMs: latencies }));

    // таймаут 30 сек
    setTimeout(() => { ws.close(); resolve({ sent, received, latencyMs: latencies }); }, 30_000);
  });
}

// ── Запуск ─────────────────────────────────────────────────────────────────
console.log(`\nStress test: ${NUM_CLIENTS} clients × ${MSGS_PER_CLIENT} msgs in room '${ROOM}'`);
console.log(`Expected deliveries: ${NUM_CLIENTS} × ${NUM_CLIENTS * (MSGS_PER_CLIENT + 1)} = ${
  NUM_CLIENTS * NUM_CLIENTS * (MSGS_PER_CLIENT + 1)
} total msg-deliveries\n`);

const startConnect = Date.now();

// Подключаем батчами по 50 чтобы не перегружать ОС
const BATCH = 50;
const promises: Promise<any>[] = [];
for (let i = 0; i < NUM_CLIENTS; i += BATCH) {
  const end = Math.min(i + BATCH, NUM_CLIENTS);
  for (let j = i; j < end; j++) promises.push(spawnClient(j));
  await Bun.sleep(50);
}

process.stdout.write(`Connecting ${NUM_CLIENTS} clients... `);
const results = await Promise.all(promises);
const elapsed = (Date.now() - startConnect) / 1000;

// ── Статистика ─────────────────────────────────────────────────────────────
let totalSent     = 0;
let totalReceived = 0;
const allLatencies: number[] = [];

for (const r of results) {
  totalSent     += r.sent;
  totalReceived += r.received;
  allLatencies.push(...r.latencyMs);
}

allLatencies.sort((a, b) => a - b);
const p50 = allLatencies[Math.floor(allLatencies.length * 0.50)] ?? 0;
const p95 = allLatencies[Math.floor(allLatencies.length * 0.95)] ?? 0;
const p99 = allLatencies[Math.floor(allLatencies.length * 0.99)] ?? 0;
const avg = allLatencies.length
  ? Math.round(allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length)
  : 0;

console.log("done\n");
console.log("══════════════════════════════════════");
console.log(`  Clients          ${NUM_CLIENTS}`);
console.log(`  Elapsed          ${elapsed.toFixed(2)}s`);
console.log(`  Msgs sent        ${totalSent}`);
console.log(`  Msgs received    ${totalReceived}`);
console.log(`  Throughput       ${Math.round(totalReceived / elapsed)} msg/s`);
console.log(`  Latency avg      ${avg}ms`);
console.log(`  Latency p50      ${p50}ms`);
console.log(`  Latency p95      ${p95}ms`);
console.log(`  Latency p99      ${p99}ms`);
console.log("══════════════════════════════════════\n");
