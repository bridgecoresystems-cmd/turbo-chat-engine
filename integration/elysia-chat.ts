/**
 * BridgeCore Chat — ElysiaJS plugin
 *
 * Add to your existing ElysiaJS app:
 *   import { chatPlugin } from "./elysia-chat"
 *   app.use(chatPlugin)
 *
 * Env vars required (same in Rust server):
 *   JWT_SECRET=your-secret-here
 *   CHAT_SERVER_URL=http://chat-server:8080   (for history proxy)
 */

import { Elysia, t } from "elysia";
import { jwt } from "@elysiajs/jwt";

const CHAT_SERVER_URL = process.env.CHAT_SERVER_URL ?? "http://127.0.0.1:8080";
const JWT_SECRET      = process.env.JWT_SECRET      ?? "dev-secret-change-in-production";

export const chatPlugin = new Elysia({ prefix: "/chat" })
  .use(
    jwt({
      name:   "chatJwt",
      secret: JWT_SECRET,
    })
  )

  /**
   * POST /chat/token
   * Call this from your auth-protected routes to issue a chat token.
   * Body: { userId, role, roomId }
   * Returns: { token }
   *
   * Example (inside your teacher/student route):
   *   const { token } = await fetch("/chat/token", {
   *     method: "POST",
   *     body: JSON.stringify({ userId: user.id, role: "teacher", roomId: "room-42" })
   *   }).then(r => r.json())
   */
  .post(
    "/token",
    async ({ body, chatJwt }) => {
      const token = await chatJwt.sign({
        sub:  body.userId,
        role: body.role,
        // exp is set by jwt plugin via expiresIn
      });
      return { token };
    },
    {
      body: t.Object({
        userId: t.String(),
        role:   t.Union([t.Literal("teacher"), t.Literal("student")]),
        roomId: t.String(),
      }),
    }
  )

  /**
   * GET /chat/history/:roomId?limit=50
   * Proxies to the Rust server history endpoint.
   * Requires the user to be authenticated in your main app.
   */
  .get(
    "/history/:roomId",
    async ({ params, query }) => {
      const limit = Math.min(Number(query.limit ?? 50), 200);
      const res   = await fetch(
        `${CHAT_SERVER_URL}/history/${params.roomId}?limit=${limit}`
      );
      return res.json();
    },
    {
      params: t.Object({ roomId: t.String() }),
      query:  t.Object({ limit: t.Optional(t.Numeric()) }),
    }
  );
