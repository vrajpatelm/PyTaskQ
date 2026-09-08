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

| # | Bug | File | Impact |
|---|-----|------|--------|
| 1 | **Redis key format is inconsistent** — keys are stored as `Task id{uuid}` (with a space, no separator). One typo anywhere = silent data loss with no error. | `worker.py:196`, `app.py:115` | Silent data loss on task lookup |
| 2 | **`matrix_multiply` uses O(n³) pure Python loops** — At size 1000 (the new limit), this can run for several minutes and starve the entire CPU process pool for all other users. | `task_registery.py:9-18` | Full process pool starvation |
| 3 | **`/task/schedule` has no backpressure check** — the 500-task queue cap can be bypassed by using the schedule endpoint instead of enqueue. | `app.py:92` | Queue cap bypass |
| 4 | **Frontend matrix validation has a typo** — checks `"matrix_multiplication"` but the actual task name is `"matrix_multiply"`. Frontend guard never fires. | `App.jsx:70` | Frontend limit is non-functional |
| 5 | **`status_box` CSS class type is `"Error"` (capitalized)** — the component expects lowercase `"error"`. Red error styling never renders. | `App.jsx:71` | UI feedback broken |
| 6 | **`ARCHITECTURE.md` documents stale API routes** — still shows `GET /task/mul?size=N` and Form Data routes that no longer exist. | `ARCHITECTURE.md:27` | Misleading documentation |

---

## 🚀 Phase 1 — Foundation: Webhook Model (Multi-Tenant)
*This is the current strategic priority. Do these in order.*

- [ ] **API Key Authentication** — FastAPI `HTTPBearer` dependency, keys stored in a Redis Set (`api_keys`). All write endpoints protected.
- [ ] **Admin Key Management Endpoints** — `POST /admin/keys/create` and `DELETE /admin/keys/revoke`, protected by a master key from `.env`.
- [ ] **Per-Tenant Task Namespacing** — Decode the API key on every request and attach the owner identity to the task payload (`owner_id`). Ensures one tenant cannot see another's tasks.
- [ ] **Webhook Registration Endpoint** — `POST /webhooks/register` — allows a tenant to register a persistent default webhook URL tied to their API key instead of passing it on every request.
- [ ] **Formalize Webhook Delivery in Worker** — The `fire_webhook` call currently runs with no retry. Treat webhook delivery as its own retry-able operation. If the destination server is down, back off and retry 3 times before logging failure.
- [ ] **Webhook Signature (HMAC-SHA256)** — Sign every outgoing webhook payload with a secret so the receiving server can verify the request genuinely came from PyTaskQ and not a spoofed attacker.
- [ ] **Update Frontend** — Add an API Key input field. Store the key in `sessionStorage`. Send it as a `Bearer` token on all requests.

---

## 🔧 Phase 2 — Observability & Reliability

- [ ] **Task `started_at` Timestamp** — Record when a task begins executing. Expose elapsed time in the dashboard.
- [ ] **Task Timeout** — Kill any task running longer than a configurable N seconds. Prevents a single bad task from holding a worker slot forever.
- [ ] **Richer `/metrics` Endpoint** — Add: tasks completed per minute (throughput), average execution time, worker count + IDs, error rate.
- [ ] **Live Task Tracking in Dashboard** — After dispatch, automatically track the returned `task_id` in a "Recent Submissions" panel that polls its status live.
- [ ] **Fix Redis Key Format** — Migrate to `task:{task_id}` everywhere. Update both `worker.py` and `app.py`.
- [ ] **Replace matrix_multiply with NumPy** — Drop the O(n³) loops. Use `np.dot()`.

---

## 💡 Phase 3 — Platform Features

- [ ] **Task Priority Queue** — Use a Redis Sorted Set scored by priority. High-priority tasks (score 1) are consumed before low-priority tasks (score 10).
- [ ] **Task Chaining** — Allow a task to define a `next_task` in its payload. When Task A completes successfully, automatically enqueue Task B with A's result as input.
- [ ] **Recurring / Cron Tasks** — Allow tenants to register a task that runs on a schedule (e.g., every 5 minutes) using a cron expression.
- [ ] **Task Search & History Page** — A dedicated dashboard page showing all tasks for a given API key, filterable by status and date.
- [ ] **Multi-Worker Horizontal Scaling Config** — A `docker-compose.scale.yml` that can spin up N worker replicas with a single command.

---

## ✅ Completed

| Feature | Notes |
|---------|-------|
| Core async task queue (BRPOPLPUSH) | Reliable, at-least-once delivery |
| CPU vs I/O pool dispatch | ProcessPoolExecutor + ThreadPoolExecutor |
| Exponential backoff retry (3x) | Delays: 2s, 4s, 8s |
| Dead Letter Queue (DLQ) + full UI | View, replay, purge individual/all |
| Worker crash recovery on startup | Sweeps own processing queue on boot |
| Zombie worker sweeper | Recovers tasks from dead workers |
| Heartbeat system | Workers register TTL in active_workers ZSET |
| Delayed / scheduled tasks | ZADD to delayed_tasks ZSET |
| Webhook delivery (basic) | fire_webhook via thread pool |
| Queue backpressure (500 cap) | 429 on /task/enqueue |
| Rate limiting (10 req/min per IP) | Redis-based sliding window |
| Matrix size limit (backend + frontend) | 1000x1000 hard cap |
| React dashboard | Metrics, dispatch, DLQ management, task lookup |
| Docker Compose deployment | Redis + API + Worker in one command |
| pytest suite | crash recovery, DLQ, graceful shutdown, handle_task |

---

## 📊 Honest CV Rating

**Current State (before Phase 1): 6.5 / 10**

It is a genuinely well-architected project with real distributed systems thinking behind it. Most bootcamp projects would not implement `BRPOPLPUSH`, a zombie sweeper, or a ZSET-based retry scheduler. Those details matter to engineers reading your CV.

What holds it back to a 6.5 is that it currently solves a problem only you have — queuing tasks for your own server. It has no auth, limited task types, and is essentially a personal tool.

**After Phase 1 (Webhook Model + API Keys): 8.5 / 10**

This becomes a genuinely impressive portfolio project. You can now say:

> *"Built a multi-tenant Task Queue as a Service with API key authentication, webhook-based result delivery with HMAC-SHA256 signature verification, exponential backoff retries, a Dead Letter Queue, and a real-time React dashboard."*

That sentence will make a senior engineer sit up in an interview. It demonstrates that you understand distributed systems, security, and multi-tenancy — concepts that most junior and mid-level developers have never thought about.

**After Phase 2 + 3: 9.5 / 10**

At that point the project is architecturally comparable to Inngest or Trigger.dev. You're no longer describing a portfolio project — you're describing a product.

---

*Last updated: 2026-09-07*
