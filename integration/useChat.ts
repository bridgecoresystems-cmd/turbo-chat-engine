/**
 * useChat — Nuxt 3 composable for BridgeCore Chat
 *
 * Usage in any page/component:
 *   const { messages, onlineUsers, send, sendTyping, isConnected } = useChat("room-42")
 *
 * Requires protobufjs:  bun add protobufjs
 */

import { ref, computed, onUnmounted } from "vue";
import * as protobuf from "protobufjs";

// ── Proto types (mirrors proto/message.proto) ────────────────────────────────

const root = protobuf.Root.fromJSON({
  nested: {
    turbo_chat: {
      nested: {
        Envelope: {
          fields: {
            message:  { id: 1, type: "ChatMessage", oneOf: "kind" },
            ack:      { id: 2, type: "Ack",         oneOf: "kind" },
            typing:   { id: 3, type: "Typing",      oneOf: "kind" },
            presence: { id: 4, type: "Presence",    oneOf: "kind" },
          },
          oneofs: { kind: { oneof: ["message", "ack", "typing", "presence"] } },
        },
        ChatMessage: {
          fields: {
            id:        { id: 1, type: "uint64" },
            room_id:   { id: 2, type: "string" },
            sender_id: { id: 3, type: "string" },
            payload:   { id: 4, type: "bytes"  },
            timestamp: { id: 5, type: "int64"  },
          },
        },
        Ack:     { fields: { message_id: { id: 1, type: "uint64" } } },
        Typing:  { fields: { room_id: { id: 1, type: "string" }, user_id: { id: 2, type: "string" }, is_typing: { id: 3, type: "bool" } } },
        Presence:{ fields: { room_id: { id: 1, type: "string" }, user_id: { id: 2, type: "string" }, status:    { id: 3, type: "string" } } },
      },
    },
  },
});

const Envelope     = root.lookupType("turbo_chat.Envelope");
const ChatMessage  = root.lookupType("turbo_chat.ChatMessage");
const TypingMsg    = root.lookupType("turbo_chat.Typing");

// ── Types ────────────────────────────────────────────────────────────────────

export interface Message {
  id:        string;
  senderId:  string;
  text:      string;
  timestamp: number;
  mine:      boolean;
}

// ── Composable ────────────────────────────────────────────────────────────────

const CHAT_WS_URL = import.meta.env.VITE_CHAT_WS_URL ?? "ws://localhost:8080";

export function useChat(roomId: string) {
  const messages     = ref<Message[]>([]);
  const onlineUsers  = ref<Set<string>>(new Set());
  const typingUsers  = ref<Set<string>>(new Set());
  const isConnected  = ref(false);

  let ws: WebSocket | null = null;
  let typingTimer: ReturnType<typeof setTimeout> | null = null;
  let myUserId = "";

  // ── Connect ────────────────────────────────────────────────────────────────
  async function connect(token: string, userId: string) {
    myUserId = userId;

    // Load history before connecting
    try {
      const history = await $fetch<Message[]>(`/chat/history/${roomId}?limit=50`);
      messages.value = history.map((m: any) => ({
        id:        String(m.id),
        senderId:  m.sender_id,
        text:      m.text,
        timestamp: m.timestamp,
        mine:      m.sender_id === userId,
      }));
    } catch {}

    ws = new WebSocket(`${CHAT_WS_URL}/?token=${token}`);
    ws.binaryType = "arraybuffer";

    ws.onopen = () => {
      isConnected.value = true;
      // Send join message
      const msg = ChatMessage.create({
        id:        BigInt(Date.now()),
        room_id:   roomId,
        sender_id: userId,
        payload:   new TextEncoder().encode("join"),
        timestamp: BigInt(Date.now()),
      });
      sendEnvelope({ message: msg });
    };

    ws.onmessage = (event) => {
      const buf = new Uint8Array(event.data);
      const env = Envelope.decode(buf) as any;

      if (env.message) {
        const text = new TextDecoder().decode(env.message.payload);
        if (text === "join" || text === "leave") return; // skip system messages
        messages.value.push({
          id:        String(env.message.id),
          senderId:  env.message.sender_id,
          text,
          timestamp: Number(env.message.timestamp),
          mine:      env.message.sender_id === myUserId,
        });
      }

      if (env.typing) {
        const uid = env.typing.user_id;
        if (uid === myUserId) return;
        if (env.typing.is_typing) {
          typingUsers.value.add(uid);
        } else {
          typingUsers.value.delete(uid);
        }
      }

      if (env.presence) {
        if (env.presence.status === "online") {
          onlineUsers.value.add(env.presence.user_id);
        } else {
          onlineUsers.value.delete(env.presence.user_id);
        }
      }
    };

    ws.onclose = () => { isConnected.value = false; };
    ws.onerror = () => { isConnected.value = false; };
  }

  // ── Send message ───────────────────────────────────────────────────────────
  function send(text: string) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const msg = ChatMessage.create({
      id:        BigInt(Date.now()),
      room_id:   roomId,
      sender_id: myUserId,
      payload:   new TextEncoder().encode(text),
      timestamp: BigInt(Date.now()),
    });
    sendEnvelope({ message: msg });
    stopTyping();
  }

  // ── Typing indicator ───────────────────────────────────────────────────────
  function sendTyping() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    sendEnvelope({ typing: TypingMsg.create({ room_id: roomId, user_id: myUserId, is_typing: true }) });
    if (typingTimer) clearTimeout(typingTimer);
    typingTimer = setTimeout(stopTyping, 3000);
  }

  function stopTyping() {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    sendEnvelope({ typing: TypingMsg.create({ room_id: roomId, user_id: myUserId, is_typing: false }) });
    if (typingTimer) { clearTimeout(typingTimer); typingTimer = null; }
  }

  // ── Helpers ────────────────────────────────────────────────────────────────
  function sendEnvelope(kind: object) {
    const buf = Envelope.encode(Envelope.create(kind)).finish();
    ws!.send(buf);
  }

  function disconnect() {
    stopTyping();
    ws?.close();
  }

  onUnmounted(disconnect);

  // "User is typing: Anna, Bob"
  const typingText = computed(() => {
    const users = [...typingUsers.value];
    if (!users.length) return "";
    return users.join(", ") + (users.length === 1 ? " пишет..." : " пишут...");
  });

  return { messages, onlineUsers, typingUsers, typingText, isConnected, connect, send, sendTyping, disconnect };
}
