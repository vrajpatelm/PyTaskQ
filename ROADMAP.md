# PyTaskQ — Living Roadmap & System Health

> This document is the single source of truth for the project's direction.
> It is updated whenever a critical bug is discovered or a significant feature lands.
> Status key: 🔴 Critical · 🟠 High · 🟡 Medium · 🟢 Done · 🚧 In Progress

---

## 🎯 North Star Goal
**Transform PyTaskQ into a multi-tenant Task Queue as a Service (TQaaS) using the Webhook Model.**

Any authenticated user with an API key should be able to:
1. Submit tasks to the queue via the REST API.
2. Register a webhook URL that receives task results when complete.
3. Run their own business logic on their own server — PyTaskQ handles the queue, retries, and delivery.

---

## 🔴 Critical Bugs (Fix Before Adding Features)

| # | Bug | File | Impact | Status |
|---|-----|------|--------|--------|
| 1 | ~~**Redis key format is inconsistent**~~ | `worker.py`, `app.py` | ~~Silent data loss on task lookup~~ | ✅ Fixed — all paths use `task:{task_id}` |
| 2 | **`matrix_multiply` uses O(n³) pure Python loops** — At size 1000 (the new limit), this can run for several minutes and starve the entire CPU process pool for all other users. | `task_registery.py:9-18` | Full process pool starvation | ✅ Fixed — uses `np.dot()` |
| 3 | ~~**`/task/schedule` has no backpressure check**~~ | `app.py:132` | ~~Queue cap bypass~~ | ✅ Fixed — `check_backpressure` added |
| 4 | ~~**Frontend matrix validation typo**~~ — checked `"matrix_multiplication"` but task is `"matrix_multiply"`. | `App.jsx` | ~~Frontend limit non-functional~~ | ✅ Fixed — corrected in App.jsx rewrite |
| 5 | ~~**`status_box` CSS class used `"Error"` (capital E)**~~ — CSS expects lowercase `"error"`. | `App.jsx` | ~~Error styling never rendered~~ | ✅ Fixed — all status types now lowercase |
| 6 | ~~**`ARCHITECTURE.md` documents stale API routes**~~ — still shows `GET /task/mul?size=N` and Form Data routes that no longer exist. | `ARCHITECTURE.md:27` | ~~Misleading documentation~~ | ✅ Fixed — docs updated |

---

## 🚀 Phase 1 — Foundation: Webhook Model (Multi-Tenant)
*This is the current strategic priority. Do these in order.*

- [x] **API Key Authentication** — FastAPI `HTTPBearer` dependency, keys stored in a Redis Set (`api_keys`). All write endpoints protected.
- [x] **Admin Key Management Endpoints** — `POST /admin/keys/create` and `DELETE /admin/keys/revoke`, protected by a master key from `.env`.
- [x] **Per-Tenant Task Namespacing** — Decode the API key on every request and attach the owner identity to the task payload (`owner_id`). Ensures one tenant cannot see another's tasks.
- [x] **Webhook Registration Endpoint** — `POST /webhooks/register` + `GET /webhooks/info`
- [x] **Formalize Webhook Delivery in Worker** — treated as its own queued task with 3 retries + DLQ
- [x] **Webhook Signature (HMAC-SHA256)** — `X-PyTaskQ-Signature` header on every delivery
- [x] **Update Frontend** — API Key login gate, sessionStorage, Bearer token on all requests

    
---

## 🚀 Phase 1.5 — Scale to 1,000 Users & Priority Queuing
*Architectural changes required to handle high traffic and important tasks seamlessly.*

- [x] **Task Priority Queue** — Three Redis Lists (`queue:high`, `queue:default`, `queue:low`) polled in strict priority order via `RPOPLPUSH`.
- [x] **Per-Tenant Backpressure** — 100 req/min rate limit per API key
- [x] **Increased Rate Limits** — 100 req/min (up from 10)
- [ ] **Horizontal Worker Scaling** — `docker-compose.yml` `replicas: N`

---

## 🔧 Phase 2 — Observability & Reliability

- [ ] **Task `started_at` Timestamp** — Record when a task begins executing. Expose elapsed time in dashboard.
- [ ] **Task Timeout** — Kill any task running longer than N seconds. Prevents a bad task holding a worker slot.
- [ ] **Worker Count in `/metrics`** — Show how many workers are alive right now (from `active_workers` ZSET).
- [x] **Fix Redis Key Format** — Migrated to `task:{task_id}` everywhere.
- [x] **Replace matrix_multiply with NumPy** — Uses `np.dot()` instead of O(n³) loops.
- [x] **Live Task Feed in Dashboard** — Last 5 dispatched tasks with live status polling every 1s.
- [x] **Richer `/metrics` Endpoint** — Queue depth per priority, rate limit usage, capacity bar.

---

## 💡 Phase 3 — Platform Features
- [ ] **Task Chaining** — Allow a task to define a `next_task` in its payload. When Task A completes successfully, automatically enqueue Task B with A's result as input.
- [ ] **Recurring / Cron Tasks** — Allow tenants to register a task that runs on a schedule (e.g., every 5 minutes) using a cron expression.
- [ ] **Task Search & History Page** — A dedicated dashboard page showing all tasks for a given API key, filterable by status and date.
- [ ] **Multi-Worker Horizontal Scaling Config** — A `docker-compose.scale.yml` that can spin up N worker replicas with a single command.

---

## ✅ Completed

| Feature | Notes |
|---------|-------|
| Core async task queue (RPOPLPUSH) | Reliable, at-least-once delivery with priority ordering |
| Priority Queue (3-tier) | queue:high → queue:default → queue:low polling |
| Per-IP multi-tenant isolation | Metrics, DLQ, and stats scoped by client IP |
| CPU vs I/O pool dispatch | ProcessPoolExecutor + ThreadPoolExecutor |
| Exponential backoff retry (3x) | Delays: 2s, 4s, 8s |
| Dead Letter Queue (DLQ) + full UI | View, replay, purge individual/all (per-IP) |
| Worker crash recovery on startup | Sweeps own processing queue on boot → queue:high |
| Zombie worker sweeper | Recovers tasks from dead workers → queue:high |
| Heartbeat system | Workers register TTL in active_workers ZSET |
| Delayed / scheduled tasks | ZADD to delayed_tasks ZSET |
| Webhook Registration + `GET /webhooks/info` | Per-tenant, persisted in Redis hash |
| Reliable webhook delivery (retry + DLQ) | Treated as queued task, 3 retries, exponential backoff |
| HMAC-SHA256 signed webhooks | `X-PyTaskQ-Signature` header, verifiable by receiver |
| Idempotency-aware rate limiting | Duplicates caught by idempotency key do not consume rate limit slots |
| Decorrelated Jitter retries (AWS Strategy 3) | Each task carries `prev_delay`; eliminates thundering herd |
| Fencing Token (Lua atomic script) | Prevents stale worker overwriting fresh result |
| Matrix size limit (backend + frontend) | 1000×1000 hard cap |
| React dashboard (Lucide icons) | Metrics, dispatch, DLQ, task lookup, webhook manager |
| Docker Compose deployment | Redis + API + Worker in one command |
| pytest suite | crash recovery, DLQ, graceful shutdown, handle_task |

---

## 📊 Honest CV Rating

**Current State: 8.5 / 10** — Phase 1 + 1.5 complete.

> *"Built a multi-tenant Task Queue as a Service with API key auth, HMAC-SHA256 signed webhooks, fencing tokens (Lua atomic scripts), decorrelated jitter retries, zombie worker recovery, and a real-time React dashboard."*

**After Phase 2 + 3: 9.5 / 10** — Architecturally comparable to Inngest or Trigger.dev.

---

*Last updated: 2026-09-25*
