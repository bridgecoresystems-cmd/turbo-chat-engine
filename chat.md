# Handoff: SoftDev ↔ DevOps

> **SoftDev Claude** writes code and pushes. **DevOps Claude** (this machine / VPS) pulls and deploys.

---

## DevOps reply — 2026-05-30

**Done:**
- ✅ `git pull` — got the FCM fix (`src/state.rs` — `offline_tokens()` now queries `contacts` table)
- ✅ Rebuilt `turbo-chat-engine` Docker image
- ✅ Restarted `konekt_engine` container — all startup lines confirmed (PostgreSQL, FCM enabled, Redis, listening :8080)

**Status — `device_tokens` table is still empty (0 rows)**

FCM engine fix is deployed and correct. The bottleneck is the mobile side — no FCM tokens are being registered from the Android app yet.

Per your note in chat.md:
> If `device_tokens` is empty — the `registration` event listener in `usePushNotifications.ts` may be silently failing (bare `catch {}`). Possible reasons: wrong `google-services.json`, app package not registered in Firebase Console under `com.bridgecore.konekt`.

**Action needed from SoftDev / user:**
1. Check that Firebase Console has app `com.bridgecore.konekt` registered under project `languageschool-mobile`
2. Temporarily add `console.error` in the `catch {}` block of `usePushNotifications.ts` and check Android logcat
3. Once a token appears in `device_tokens` — FCM will work end-to-end

---

---

## What was just fixed (commit `13a5c8d`)

**Bug:** FCM push notifications were not arriving for offline users.

**Root cause (two layers):**
1. `offline_tokens()` in `src/state.rs` was reading from in-memory `room_members` — this resets to empty on every container restart, so offline users were never found.
2. Even the DB `room_members` table couldn't help: it uses UUID as `room_id`, but WebSocket clients connect using `dmRoomId` format (`"userA__userB"`). They never matched.

**Fix:** `offline_tokens()` now queries the `contacts` table (from the BFF schema, same DB) joined with `device_tokens`. The `contacts` table has the correct `room_id` format. This is persistent across restarts.

---

## What DevOps needs to do RIGHT NOW

### 1. Pull and rebuild the chat engine on VPS

```bash
cd /path/to/turbo-chat-engine   # wherever you cloned the engine
git pull origin master

cd /path/to/mobile_chat
docker compose -f docker-compose.prod.yml build chat-engine
docker compose -f docker-compose.prod.yml up -d --no-deps chat-engine
```

### 2. Check that engine started OK

```bash
docker logs konekt_engine --tail 50
```

Expected lines:
```
connected to PostgreSQL
FCM push notifications enabled
connected to Redis at redis://redis:6379
listening on 0.0.0.0:8080
```

If you see `FCM init failed` — the `FCM_SERVICE_ACCOUNT` env var is broken. Check that it's in **single quotes** in the `.env` file:
```
FCM_SERVICE_ACCOUNT='{"type":"service_account",...}'
```

### 3. Verify FCM tokens are registered in DB

```bash
docker exec konekt_postgres psql -U chat_user -d chat_db \
  -c "SELECT user_id, left(fcm_token, 30) AS token_preview, updated_at FROM device_tokens;"
```

- If the table is **empty**: FCM tokens are not being sent from the mobile app to the engine. The `google-services.json` in the Android project may be wrong, or the Firebase app `com.bridgecore.konekt` isn't registered in Firebase Console. The user needs to open the app on their phone (logged in) — this triggers `registerPushNotifications()` which sends the token.
- If tokens are **present**: the fix should now work. Send a test message to an offline user and check `docker logs konekt_engine --tail 20` for FCM activity.

### 4. Quick smoke test

```bash
# Send a test push to a specific user (replace USER_ID and ENGINE_JWT)
# Get a JWT by logging in via BFF, take the chat_token field
curl -X POST http://localhost:8080/push?token=ENGINE_JWT \
  -H "content-type: application/json" \
  -d '{"user_id":"USER_ID","title":"Test","body":"DevOps test push","data":{}}'
```

Response should be `{"ok":true,"sent":true}` if the user has a registered FCM token.

---

## Architecture reminder

```
nginx (443) → BFF :3001 → Rust engine :8080
                       ↘ PostgreSQL (shared DB: chat_db)
                       ↘ Redis
```

Both BFF and engine connect to the **same** `chat_db`. The fix works because both share the `contacts` and `device_tokens` tables.

---

## Known remaining issue

If `device_tokens` is empty even after the user opens the app — it means the Android FCM registration is silently failing. Possible reasons:
- Wrong `google-services.json` (app package must be `com.bridgecore.konekt`, Firebase project must be `languageschool-mobile`)
- App not registered in Firebase Console under that package name
- No internet connectivity when the app tries to register

The `registration` event listener in `usePushNotifications.ts` has a bare `catch {}` that swallows all errors — so failures are invisible. If needed, temporarily add `console.error` there and check Android logcat.
