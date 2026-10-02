# PyTaskQ — Interview Prep & Honest Review

Everything you need to explain this project in an interview: what it does, how each part works, what's genuinely good, what's lacking (and how to talk about it), plus videos/docs to study.

---

## 1. The 60-Second Pitch (memorize this)

> "PyTaskQ is a distributed async task queue I built from scratch with FastAPI, Redis, and asyncio — essentially a mini Celery. Clients submit tasks over HTTP; workers pull them from three Redis priority lists and execute CPU-bound tasks in a ProcessPoolExecutor and I/O-bound tasks in a ThreadPoolExecutor. I focused on the distributed-systems hard parts: at-least-once delivery via RPOPLPUSH with per-worker processing queues, crash recovery plus a zombie sweeper that recovers tasks from dead workers using heartbeats, fencing tokens enforced by a Lua script so a stale worker can't overwrite a re-assigned task's result, retries with decorrelated-jitter backoff into a delayed ZSET, and a Dead Letter Queue with replay/purge endpoints. The API has two-layer idempotency (client idempotency keys + a content-hash safety net), per-tenant API keys with rate limiting, backpressure on queue capacity, and full OpenTelemetry tracing from API to worker across Redis using W3C trace context."

That single paragraph touches: queues, concurrency, GIL, delivery guarantees, idempotency, consensus-ish fencing, backoff, DLQ, multi-tenancy, observability. It gives the interviewer many doors to walk through — all of which you've prepared below.

---

## 2. Architecture (component → file → what to say)

### Big picture

```
Client ──HTTP──▶ FastAPI (app.py) ──LPUSH──▶ Redis ──RPOPLPUSH──▶ Worker (worker.py)
   ▲                 │                        ▲                       │
   │                 │                        │                       ├── ProcessPoolExecutor (cpu tasks)
   │                 ├── WebSocket dashboard  │                       └── ThreadPoolExecutor (io tasks)
   └─────────────────┘   (Redis Pub/Sub)      │
                          delayed_tasks ZSET ◀┴── retry scheduler (re-queues when due)
```

### A. API layer — [src/app.py](src/app.py)

| Feature | Where | Interview soundbite |
|---|---|---|
| Priority routing | `LPUSH queue:{high\|default\|low}` | 3 lists polled in strict order — simpler than a ZSET per task, no score collisions, O(1) push |
| Rate limiting | `rate_limiter` + `_incr_rate_limit` | Check-only dependency, then increment *after* idempotency passes so retries don't burn quota |
| Backpressure | `check_backpressure` | Sums the 3 queue depths vs `QUEUE_CAPACITY` → 429 "server busy" instead of unbounded memory |
| Idempotency (Layer 1) | `check_idempotency` | `Idempotency-Key` header or body field; `SET NX EX` claims the key, cached response replayed for duplicates |
| Idempotency (Layer 2) | same fn | Content-hash (task_name + args + tenant, SHA-256, 16 hex chars) blocks accidental identical resubmits for 5s |
| Multi-tenancy | `authenticate` | API key → `tenant_id` in a Redis hash; stats, DLQ, metrics namespaced per tenant |
| Real-time dashboard | `/ws/dashboard` | Redis Pub/Sub per tenant (`events:{tenant}`) wakes a pusher task; 30s heartbeat fallback; clients can "track" up to 10 task IDs |
| Graceful Redis outage | exception handler | `RedisConnectionError` → clean HTTP 503 |

### B. Worker — [src/worker.py](src/worker.py)

| Feature | Where | Interview soundbite |
|---|---|---|
| Reliable fetch | `RPOPLPUSH queue:X processing_queue:{WORKER_ID}` | Atomic pop + backup-list push = at-least-once delivery; crash leaves the task in the processing queue |
| Crash recovery | startup sweep in `consumer_task` | On boot, drains own processing queue back to `queue:high` |
| Zombie sweeper | `zombie_sweeper` | Heartbeats in `active_workers` ZSET (score = expiry). Workers whose lease expires get their in-flight tasks recovered — and their `fence_token` bumped |
| Fencing tokens | `FENCE_SCRIPT` (Lua) | Before saving a result, `EVAL` checks the task's fence token in Redis matches ours; stale workers lose the write. This is the fix for "zombie worker writes after failover" |
| Retry with jitter | `handle_task` failure path | Decorrelated jitter (AWS style): `delay = min(30, uniform(1, prev*3))`, `prev_delay` stored *on the task payload* so it survives the Redis round-trip |
| DLQ | `handle_task` | After 3 failed attempts → `dlq:{tenant_id}` + `stats:dlq` + task hash marked `DeadLetter`; API offers replay/purge |
| Concurrency control | `asyncio.Semaphore(10)` + pools | Semaphore bounds in-flight tasks; process pool (4) bypasses GIL for CPU, thread pool (10) overlaps I/O waits |
| Graceful shutdown | signal handlers | SIGINT/SIGTERM stop intake, `gather` in-flight tasks, shut pools, close Redis, flush OTel spans |
| Tracing | `trace_carrier` | OpenTelemetry W3C `traceparent` serialized into the task JSON so an API span and worker span join into one trace in Jaeger |

### C. Task registry — [src/task_registery.py](src/task_registery.py)

A dict of `name → {handler, type}`. The worker dispatches on `type` (`cpu`/`io`); the API never knows how a task executes. `matrix_multiply` uses `np.dot` (vectorized — a naive O(n³) Python loop would starve the whole process pool). Webhook delivery is itself a task (`_deliver_webhook`) signed with HMAC-SHA256 (`X-PyTaskQ-Signature`).

---

## 3. Rating (honest)

| Dimension | Score | Why |
|---|---|---|
| Ambition / feature breadth | **8.5/10** | Priority queues, retries+jitter, DLQ, idempotency, fencing, multi-tenancy, rate limits, backpressure, webhooks, WS dashboard, OTel, crash recovery — this is a lot, and it mostly works |
| Distributed-systems understanding | **8/10** | Fencing tokens + Lua, at-least-once via RPOPLPUSH, heartbeats/zombie recovery, decorrelated jitter — these are the concepts interviewers love, implemented for real |
| Test correctness | **9/10** | 61 tests, all green — covers queuing, worker paths, DLQ, crash recovery, shutdown, auth, dedup, tenant isolation, SSRF guard |
| Production readiness | **7/10** | Secrets hygiene fixed, SSRF guarded, tenant isolation fail-closed, rate-limit config unified; remaining: Redis SPOF (no AOF/Sentinel yet), no CI, no task timeout |
| Code quality & docs | **8/10** | Docs consolidated in `docs/` and synced to code; config fully env-driven including load tests |
| **Overall as a portfolio project** | **≈ 8.5/10** | Top-quartile scope, hardened and documented; the remaining wins (AOF, Sentinel, CI) are listed in docs/ROADMAP.md |

---

## 4. What is lacking (the honest list)

### ✅ Recently fixed (great "found and fixed in my own system" stories)

1. **Test suite was red — now 61/61 green.** Root cause was `fakeredis` lacking Lua `EVAL` support (needed `fakeredis[lua]`), plus stale tests predating API-key auth and the jitter backoff. An interviewer who runs `pytest` now sees green.
2. **A real-looking `MASTER_KEY` had leaked into `.env.example`** — auto-copied into every clone's `.env` by the start scripts. Fixed: template emptied, local key rotated, and the admin check now uses constant-time `secrets.compare_digest`.
3. **Rate-limit config drift** — the enforcer used a benchmark `10000` while `/metrics` reported `100`. Fixed: one env-driven constant (`RATE_LIMIT_PER_MINUTE`) read by both paths, so they cannot disagree.

### ✅ Security gaps — fixed (your strongest interview stories)

4. **Cross-tenant task reads** *(was open)* — most task-hash writes didn't store `tenant_id`, so any tenant could read any task by UUID. Fixed: every write path (including the Lua fence script) stores `tenant_id`, and `GET /task/{id}` fails closed (403 on missing/mismatch).
5. **SSRF in `resize_image` / `url_health_check`** *(was open)* — workers fetched any user URL, including `169.254.169.254` and `gopher://`. Fixed: DNS-validating `_assert_safe_url()` guard, per-redirect re-validation, scheme allowlist, 10 MB download cap — 15 dedicated tests.
6. **Unbounded delayed set** *(still open)*. Backpressure counts only the 3 queues, not `delayed_tasks` — retries can grow memory forever during an outage storm.

### 🟡 Correctness / robustness nits (good "what would you improve" answers)

7. **`matrix_multiply` guard crashes on empty args** — `int(request.args[0])` raises `IndexError` → 500 instead of 422. Validate `args` length in Pydantic or check `len(args)`.
8. **Validation-failure tasks are written to `task:Unknown`** — pollutes a shared key. Use a dedicated `failed_intake` list and log.
9. **Retry scheduler race:** `zrangebyscore` then `zrem` across multiple workers is *mostly* safe (zrem acts as the lock) but requeue/duplicate-handling relies on fencing downstream — worth being able to explain.
10. **Shutdown isn't exception-safe:** if `gather` or pool shutdown raises, Redis/OTel cleanup can be skipped. Wrap in `try/finally`.
11. **Dead code / drift:** unused `Taskresult` schema, `Form`/`FileResponse` imports, docs (`ARCHITECTURE.md`) describing the older IP-based tenancy and 10 req/min limit, root `dump.rdb` and `result.json` committed.
12. **No CI.** A GitHub Actions workflow running `pytest` + `ruff` would have caught #1 immediately — and it's a 10-line file that looks great in an interview.
13. **Redis is a single point of failure** — say this proactively: production path is Redis Sentinel/Cluster or moving to a broker with replication; the app code barely changes because it's all standard Redis commands.
14. **Pydantic `trace_carrier: dict = {}`** — mutable default *is* safe under Pydantic (it deep-copies defaults), but know that this is Pydantic-specific; the same line in a dataclass would be the classic shared-mutable-state bug.

> In an interview, don't hide these — they're your best material. "Here's a bug I found in my own system, here's why it happens, here's how I'd fix it" beats a fake flawless demo every time.

---

## 5. Questions you WILL be asked (with answers)

1. **Why Redis lists and not Redis Streams or RabbitMQ?**
   Lists are the simplest primitive with the RPOPLPUSH reliable-queue pattern; Streams add consumer groups, acks, and replay but more complexity; RabbitMQ adds another moving part. For learning + moderate throughput, lists are right; I'd move to Streams for message replay and easier consumer scaling.
2. **At-least-once vs at-most-once vs exactly-once?**
   RPOPLPUSH gives at-least-once: crash between pop and completion → task recovered (possibly re-executed). Exactly-once doesn't exist end-to-end; I approximate "exactly-once effects" with fencing tokens on result writes and idempotency keys on intake.
3. **Walk me through the fencing token.** *(This is your strongest story)*
   Zombie sweeper recovers a dead worker's task and increments `fence:{task_id}`. The old worker may still be running; when it finishes, its Lua script checks `GET fence:{task_id} == my_token` and HSETs the result *atomically*. Stale worker's write returns 0 → discarded. Cite Kleppmann's "How to do distributed locking".
4. **Why decorrelated jitter instead of plain exponential backoff?**
   Pure backoff synchronizes retry storms (everyone retries at t+2^n). Decorrelated jitter (AWS): `sleep = min(cap, random(base, prev*3))` spreads retries and adapts to the previous delay. The previous delay travels *inside the task payload* so it survives Redis round-trips.
5. **Explain your two-layer idempotency.**
   Layer 1: client `Idempotency-Key` → `SET NX EX` claims it; duplicates within TTL get the cached response ("pending" while in flight → 409). Layer 2: no header → SHA-256 content hash blocks identical resubmits for 5s as a safety net. Stripe's idempotency blog is the reference design.
6. **GIL: why two executors?**
   Threads can't run Python bytecode in parallel (GIL), so CPU tasks go to a ProcessPoolExecutor (separate interpreters/cores); I/O tasks release the GIL while waiting, so ThreadPoolExecutor overlaps waits cheaply. That's also why `matrix_multiply` uses NumPy — vectorized C releases the GIL and avoids O(n³) Python loops.
7. **How does a task survive a worker crash? vs an API crash? vs Redis crash?**
   Worker crash → processing queue + zombie sweeper. API crash after LPUSH → task already in Redis, safe; before LPUSH → client retries with idempotency key. Redis crash → tasks lost unless persistence (AOF/RDB) is on — currently a single point of failure; Sentinel/Cluster is the fix.
8. **How do you prevent one tenant from starving others?**
   Per-tenant rate limits + backpressure + per-tenant stats/DLQ namespaces. Honest next step: per-tenant queue shares (weighted fair queuing) — right now a flood of high-priority tasks delays everyone behind the same list.
9. **Why is the webhook a task itself?**
   Delivering a webhook is retryable I/O — so it reuses the queue's retry/DLQ machinery instead of blocking the original task. HMAC signature lets receivers verify authenticity.
10. **How does tracing survive the queue hop?**
    `inject_trace_context()` serializes the current W3C traceparent into the task JSON; the worker `extract`s it and starts a child span — one continuous trace in Jaeger across HTTP → Redis → process pool.

---

## 6. Study resources (videos & docs)

### Watch (YouTube)
- **Distributed Task Scheduler: System Design Interview** — <https://www.youtube.com/watch?v=zl8BPH74GY4> — the exact problem this repo answers; watch to structure your story.
- **Design a Distributed Job Scheduler (Airflow-style)** — <https://www.youtube.com/watch?v=hLvB2haod5w> — delayed/scheduled tasks, exactly your `delayed_tasks` ZSET.
- **Designing Idempotent APIs (Stripe approach)** — <https://www.youtube.com/watch?v=J2IcD9FZvZU> — pairs with Layer 1 of your idempotency.
- **Celery: High-Performance Distributed & Async Task Queues** — <https://www.youtube.com/watch?v=v-Snbz3WmJU> — compare your design to the industry standard.
- **Python concurrency (GIL, threads vs processes)** — <https://www.youtube.com/watch?v=lONAGcOY_x8> — backs your executor story.

### Read (canonical)
- **Stripe — Designing robust and predictable APIs with idempotency**: <https://stripe.com/blog/idempotency>
- **Martin Kleppmann — How to do distributed locking** (fencing tokens; your Lua script is this): <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>
- **AWS — Exponential Backoff and Jitter** (source of your retry formula): <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>
- **Hello Interview — Redis deep dive** (queues, locks, rate limiting, and the tradeoffs interviewers probe): <https://www.hellointerview.com/learn/system-design/deep-dives/redis>
- **Redis RPOPLPUSH / reliable queue pattern**: <https://redis.io/docs/latest/commands/rpoplpush/>
- **Real Python — Speed up your Python with concurrency** (GIL, pools): <https://realpython.com/python-concurrency/>
- **Celery docs** (compare features: visibility timeout, acks_late, broker failover): <https://docs.celeryq.dev/>
- **OpenTelemetry Python docs** (your tracing setup): <https://opentelemetry.io/docs/languages/python/>
- **Book**: *Designing Data-Intensive Applications* — ch. 8 (faults/partial failure) and ch. 11 (streams) map 1:1 to this project.

### 7-day study plan
1. **Day 1–2** — Read Stripe idempotency + Kleppmann; annotate both code paths in `app.py`/`worker.py` with the article's vocabulary.
2. **Day 3** — Skim the test suite: be able to explain what `test_ssrf_guard.py` and the tenant-isolation tests prove.
3. **Day 4** — Read AWS jitter blog; be able to derive the formula from memory and explain `prev_delay` in the payload.
4. **Day 5** — Watch the two scheduler videos; redo your 60-second pitch against them.
5. **Day 6** — Practice Q&A (section 5) out loud; record yourself.
6. **Day 7** — Add GitHub Actions CI (the last structural gap) and walk through the Redis Sentinel design in SCALING_ARCHITECTURE.md.

---

*Updated 2026-09-30. Scores are one honest assessment — the remaining wins are Redis persistence/HA and CI (see docs/ROADMAP.md).*
