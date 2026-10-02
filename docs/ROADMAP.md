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
| 2 | ~~**`matrix_multiply` uses O(n³) pure Python loops**~~ | `task_registery.py` | ~~Full process pool starvation~~ | ✅ Fixed — uses `np.dot()` |
| 3 | ~~**`/task/schedule` has no backpressure check**~~ | `app.py` | ~~Queue cap bypass~~ | ✅ Fixed — `check_backpressure` added |
| 4 | ~~**Frontend matrix validation typo**~~ | `App.jsx` | ~~Frontend limit non-functional~~ | ✅ Fixed — corrected in App.jsx rewrite |
| 5 | ~~**`status_box` CSS class used `"Error"` (capital E)**~~ | `App.jsx` | ~~Error styling never rendered~~ | ✅ Fixed — all status types now lowercase |
| 6 | ~~**Docs documented stale API routes**~~ | `ARCHITECTURE.md` | ~~Misleading documentation~~ | ✅ Fixed — docs rewritten, all moved to `docs/` |
| 7 | ~~**Cross-tenant task reads**~~ — most task-hash writes never stored `tenant_id`, so `GET /task/{id}` served any task to any tenant | `worker.py`, `app.py` | ~~Tenant data leak~~ | ✅ Fixed — every write path stores `tenant_id` (incl. Lua fence script); reads fail closed (403) |
| 8 | ~~**SSRF in URL-fetching tasks**~~ — workers fetched any user URL incl. `169.254.169.254` and `gopher://` | `task_registery.py` | ~~Internal network access from workers~~ | ✅ Fixed — DNS-validating guard + redirect re-validation + scheme allowlist + 10 MB download cap |
| 9 | ~~**Real MASTER_KEY in `.env.example`**~~ — auto-copied into every clone's `.env` | `.env.example` | ~~Shared admin secret across deployments~~ | ✅ Fixed — template emptied, local key rotated, constant-time compare |
| 10 | ~~**Rate-limit config drift**~~ — enforcer used benchmark `10000`, `/metrics` reported `100` | `app.py` | ~~Observability lied to operators~~ | ✅ Fixed — single `RATE_LIMIT_PER_MINUTE` env var read by both paths |
| 11 | ~~**Hardcoded API key + host in load tests**~~ | `tests/load/` | ~~Secret in source~~ | ✅ Fixed — both locustfiles read `LOADTEST_*` from `.env` |

---

## 🚀 Phase 1 — Foundation: Webhook Model (Multi-Tenant) — ✅ COMPLETE

- [x] **API Key Authentication** — FastAPI `HTTPBearer` dependency, keys stored in Redis (`api_key:{key}` hashes). All tenant endpoints protected.
- [x] **Admin Key Management Endpoints** — `POST /admin/keys/create`, `GET /admin/keys`, `DELETE /admin/keys/revoke/{api_key}`, protected by `MASTER_KEY` (constant-time compare) from `.env`.
- [x] **Per-Tenant Task Namespacing** — every task payload and every task-hash write carries `tenant_id`; stats, DLQ, and metrics are per-tenant; result reads fail closed.
- [x] **Webhook Registration Endpoint** — `POST /webhooks/register` + `GET /webhooks/info`
- [x] **Formalize Webhook Delivery in Worker** — treated as its own queued task with 3 retries + DLQ
- [x] **Webhook Signature (HMAC-SHA256)** — `X-PyTaskQ-Signature` header on every delivery
- [x] **Update Frontend** — API Key login gate, sessionStorage, Bearer token on all requests

---

## 🚀 Phase 1.5 — Scale & Priority Queuing — ✅ COMPLETE

- [x] **Task Priority Queue** — Three Redis Lists (`queue:high`, `queue:default`, `queue:low`) polled in strict priority order via `RPOPLPUSH`.
- [x] **Env-Configurable Rate Limit** — `RATE_LIMIT_PER_MINUTE` (default 100/min), enforced and reported from one constant.
- [x] **Backpressure** — global `QUEUE_CAPACITY` (env) across all three queues → 429 when full.
- [ ] **Per-Tenant Backpressure** — the queue cap is still global; two spammy tenants can fill it. (See SCALING_ARCHITECTURE.md §2.)
- [ ] **Horizontal Worker Scaling** — `docker-compose.yml` `replicas: N`

---

## 🔧 Phase 2 — Observability & Reliability

- [x] **Fix Redis Key Format** — Migrated to `task:{task_id}` everywhere.
- [x] **Replace matrix_multiply with NumPy** — Uses `np.dot()` instead of O(n³) loops.
- [x] **Live Task Feed in Dashboard** — Last 5 dispatched tasks with live status polling every 1s.
- [x] **Richer `/metrics` Endpoint** — Queue depth per priority, rate limit usage, capacity bar, live worker count (`active_workers` ZSET).
- [x] **WebSocket Dashboard** — Redis Pub/Sub event-driven push with 30s heartbeat; tenants can track up to 10 task IDs.
- [x] **Decorrelated Jitter retries (AWS Strategy 3)** — Each task carries `prev_delay`; eliminates thundering herd.
- [x] **Fencing Token (Lua atomic script)** — Prevents stale worker overwriting fresh result; token bumped on recovery AND on timeout.
- [x] **SSRF Guard** — DNS-based URL validation on all user-fetched URLs.
- [x] **Tenant-Isolated Results** — `tenant_id` on every write; fail-closed reads.
- [ ] **Task `started_at` Timestamp** — Record when a task begins executing. Expose elapsed time in dashboard.
- [x] **Task Timeout** — `asyncio.wait_for` + fence-token invalidation (`TASK_TIMEOUT` env, default 300s); hung tasks can no longer deadlock a worker.
- [x] **Redis Persistence (AOF)** — `appendfsync everysec` + RDB snapshots + compose volume; tasks survive restarts.
- [ ] **Redis HA (Sentinel)** — remove the single point of failure.
- [ ] **CI (GitHub Actions)** — run pytest + ruff on every push.

---

## 💡 Phase 3 — Platform Features
- [ ] **Task Chaining** — Allow a task to define a `next_task` in its payload. When Task A completes successfully, automatically enqueue Task B with A's result as input.
- [ ] **Recurring / Cron Tasks** — Allow tenants to register a task that runs on a schedule (e.g., every 5 minutes) using a cron expression.
- [ ] **Task Search & History Page** — A dedicated dashboard page showing all tasks for a given tenant, filterable by status and date.
- [ ] **Multi-Worker Horizontal Scaling Config** — A `docker-compose.scale.yml` that can spin up N worker replicas with a single command.

---

## ✅ Completed

| Feature | Notes |
|---------|-------|
| Core async task queue (RPOPLPUSH) | Reliable, at-least-once delivery with priority ordering |
| Priority Queue (3-tier) | queue:high → queue:default → queue:low polling |
| API-key multi-tenant isolation | Metrics, DLQ, stats, and task results scoped per tenant; fail-closed reads |
| CPU vs I/O pool dispatch | ProcessPoolExecutor + ThreadPoolExecutor |
| Decorrelated jitter retry (3x) | `min(30, uniform(1, prev*3))`, `prev_delay` persisted in payload |
| Dead Letter Queue (DLQ) + full UI | View, replay, purge individual/all (per-tenant) |
| Worker crash recovery on startup | Sweeps own processing queue on boot → queue:high |
| Zombie worker sweeper | Recovers tasks from dead workers → queue:high, bumps fence tokens |
| Heartbeat system | Workers register TTL lease in active_workers ZSET |
| Delayed / scheduled tasks | ZADD to delayed_tasks ZSET |
| Two-layer idempotency | Client keys (header/body) + 5s content-hash net; rate-limit-fair |
| Env-driven rate limiting | `RATE_LIMIT_PER_MINUTE` — one constant for enforce + report |
| Fencing Token (Lua atomic script) | Stale-worker write protection, token bump on recovery |
| SSRF guard on URL tasks | DNS validation, redirect re-checks, scheme allowlist, 10 MB cap |
| Webhook Registration + `GET /webhooks/info` | Per-tenant, persisted in Redis hash |
| Reliable webhook delivery (retry + DLQ) | Treated as queued task, 3 retries, jitter backoff |
| HMAC-SHA256 signed webhooks | `X-PyTaskQ-Signature` header, verifiable by receiver |
| OpenTelemetry tracing | API→worker spans joined via W3C trace context in payload |
| WebSocket real-time dashboard | Pub/Sub-driven push, 30s heartbeat, task tracking |
| Matrix size limit (backend + frontend) | 1000×1000 hard cap |
| React dashboard (Lucide icons) | Metrics, dispatch, DLQ, task lookup, webhook manager |
| Docker Compose deployment | Redis + API + Worker in one command |
| pytest suite — 61 passing | queuing, worker paths, DLQ, crash recovery, shutdown, SSRF guard, auth, dedup, tenant isolation |
| Env-driven load tests | locustfile + benchmark read `LOADTEST_*` from `.env`, no secrets in source |
| Consolidated docs | All `.md` in `docs/`, synced to code |

---

## ⚠️ Known Limitations (Honest List)

1. **Redis is a single availability point.** AOF persistence now protects data across restarts; Sentinel is the remaining step for automatic failover.
2. **Backpressure is global, not per-tenant.** See Phase 1.5.
3. **No CI pipeline yet.** Tests are green (65/65) but only run locally.
4. **No task cancel endpoint.** A timed-out task is handled automatically, but ops can't manually abort a queued task yet.
5. **`delayed_tasks` ZSET is unbounded** during retry storms; backpressure doesn't count it.

---

## 📊 Honest CV Rating

**Current State: 8.5 / 10** — Phase 1 + 1.5 complete, security hardening done (tenant isolation, SSRF, secrets hygiene), docs synced, 61 tests green.

> *"Built a multi-tenant Task Queue as a Service with API key auth, HMAC-SHA256 signed webhooks, fencing tokens (Lua atomic scripts), decorrelated jitter retries, zombie worker recovery, SSRF-hardened task handlers, fail-closed tenant isolation, and a real-time React dashboard."*

**After Phase 2 remainder (AOF, Sentinel, CI, timeouts): 9.5 / 10** — Architecturally comparable to Inngest or Trigger.dev.

---

*Last updated: 2026-09-30*
