import WebSocket from "ws";
import protobuf from "protobufjs";
import path from "path";

const PROTO_PATH = path.resolve(__dirname, "../proto/message.proto");
const SERVER_URL = "ws://localhost:8080";

async function main() {
  const root = await protobuf.load(PROTO_PATH);
  const Envelope = root.lookupType("turbo_chat.Envelope");

  function makeEnvelope(id: number, roomId: string, senderId: string, text: string) {
    const msg = Envelope.create({
      kind: "message",
      message: {
        id,
        roomId,
        senderId,
        payload: Buffer.from(text, "utf8"),
        timestamp: Date.now(),
      },
    });
    return Buffer.from(Envelope.encode(msg).finish());
  }

  function decodeEnvelope(data: Buffer) {
    const env = Envelope.decode(data) as any;
    if (env.message) {
      return {
        sender: env.message.senderId,
        room: env.message.roomId,
        text: Buffer.from(env.message.payload).toString("utf8"),
      };
    }
    return null;
  }

  function connect(name: string, roomId: string): Promise<WebSocket> {
    return new Promise((resolve) => {
      const ws = new WebSocket(SERVER_URL);

      ws.on("open", () => {
        console.log(`[${name}] connected, joining room '${roomId}'`);
        // first frame = join message
        ws.send(makeEnvelope(Date.now(), roomId, name, `${name} joined`));
        resolve(ws);
      });

      ws.on("message", (data: Buffer) => {
        const msg = decodeEnvelope(data);
        if (msg) {
          console.log(`[${name} receives] ${msg.sender}: "${msg.text}"`);
        }
      });

      ws.on("error", (e) => console.error(`[${name}] error:`, e.message));
      ws.on("close", () => console.log(`[${name}] disconnected`));
    });
  }

  // ── Сценарий: teacher и student в одной комнате ───────────────────────────
  const ROOM = "chat:teacher_1:student_42";

  const teacher = await connect("teacher_1", ROOM);
  const student = await connect("student_42", ROOM);

  // небольшая задержка чтобы оба подключились
  await Bun.sleep(300);

  console.log("\n--- student пишет учителю ---");
  student.send(makeEnvelope(1, ROOM, "student_42", "Здравствуйте! Не понимаю задание 3"));

  await Bun.sleep(300);

  console.log("\n--- teacher отвечает ---");
  teacher.send(makeEnvelope(2, ROOM, "teacher_1", "Привет! Сейчас объясню..."));

  await Bun.sleep(500);

  teacher.close();
  student.close();
}

main().catch(console.error);
