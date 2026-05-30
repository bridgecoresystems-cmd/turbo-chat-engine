# framework (turbo-chat-engine)

Rust WebSocket chat engine. Used as backend by `mobile_chat` project.

## Role separation

- **This Claude instance (dev machine)** = Software Developer — writes features, fixes bugs, commits, pushes code.
- **Claude on VPS (`207.180.211.68`)** = DevOps — pulls code, rebuilds Docker images, manages containers, checks logs.

When you need the DevOps Claude to do something, see `chat.md` in this repo for the handoff note.

## Stack
- Rust 1.95+, Tokio async runtime
- `fastwebsockets` — WebSocket server
- `hyper` — HTTP server (REST endpoints)
- `sqlx` — PostgreSQL async ORM
- `redis` — async Redis client
- `aws-sdk-s3` — Cloudflare R2 file storage
- `dotenvy` — `.env` loading

## Running locally

```bash
cargo run
```

Requires `.env` in project root. See `.env` for all vars.

Local ports used by Docker:
- PostgreSQL: `chat_postgres` container on host port **5434** (`DATABASE_URL=postgres://...@127.0.0.1:5434/chat_db`)
  - Or system postgres on 5432 — check which one is active
- Redis: system redis on **6379** (`REDIS_URL=redis://127.0.0.1:6379`)
  - Docker `chat_redis` is on host port 6381 — NOT used by engine in dev

## Key env vars

```env
DATABASE_URL=postgres://chat_user:chat_pass@127.0.0.1:5432/chat_db
REDIS_URL=redis://127.0.0.1:6379
JWT_SECRET=dev-secret-change-in-production
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET=turbo-chat-files
R2_PUBLIC_URL=https://pub-840156afd60e467c98a4dcb768cfca79.r2.dev
FCM_SERVICE_ACCOUNT='{"type":"service_account","project_id":"languageschool-mobile",...}'
```

### FCM_SERVICE_ACCOUNT — CRITICAL
Must be wrapped in **single quotes** in `.env`. `dotenvy` strips inner double quotes from unquoted JSON values, making the JSON invalid and causing a panic on startup.

```env
# CORRECT:
FCM_SERVICE_ACCOUNT='{"type":"service_account",...}'

# WRONG — inner quotes will be stripped:
FCM_SERVICE_ACCOUNT={"type":"service_account",...}
```

## HTTP endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/auth/register` | Register user |
| POST | `/auth/login` | Login, returns JWT |
| GET | `/history?room_id=&before=&limit=` | Message history |
| POST | `/rooms` | Create/get DM room |
| GET | `/rooms` | List user rooms |
| POST | `/upload` | Upload file to R2 |
| POST | `/push` | Send FCM push to one user `{user_id, title, body, data}` |
| POST | `/broadcast` | Send FCM push to all users `{title, body, data}` |
| GET | `/ws` | WebSocket upgrade |

## R2 uploads — CRITICAL
Use **direct `put_object`** via aws-sdk-s3, NOT presigned PUT URLs.
Presigned PUTs from R2 break with browser CORS even when CORS is configured on the bucket.
The BFF receives the file, calls the engine's `/upload`, engine does `put_object` server-side.

## FCM push flow
1. Client connects via WebSocket → sends FCM token → engine stores in Redis (`fcm:{user_id}`)
2. When message arrives and recipient is offline → engine calls `send_fcm_to_offline()`
3. BFF calls `POST /push` when contact request is sent/accepted
4. Admin panel calls `POST /broadcast` for mass push

## State (src/state.rs)
- `AppState` holds: DB pool, Redis connection, in-memory room→connections map
- `fcm_token_for_user(user_id)` — looks up Redis `fcm:{user_id}`
- `all_fcm_tokens()` — scans Redis for all `fcm:*` keys

## Production Docker
Built from this repo's `Dockerfile`. In `docker-compose.prod.yml` it's the `chat-engine` service.
Inside Docker network: DB at `postgres:5432`, Redis at `redis:6379`.
