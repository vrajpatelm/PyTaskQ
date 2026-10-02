# Scaling PyTaskQ to 1,000+ Users

This document outlines the architectural shifts that move PyTaskQ from a "single-server internal tool" to a high-traffic "Multi-Tenant Task Queue" capable of handling 1,000+ users simultaneously.

## 1. ✅ Task Priority Queues (DONE)

**Approach chosen: Multiple Lists**

The system uses three separate Redis Lists consumed in strict priority order:
- `queue:high` — Urgent tasks (emails, webhooks, crash-recovered tasks)
- `queue:default` — Standard user tasks
- `queue:low` — Background / batch work

The worker polls these in order using `RPOPLPUSH`, ensuring `queue:high` is always drained before `queue:default` is touched. If all queues are empty, the worker sleeps for 1 second before polling again.

## 2. 🚧 Per-Tenant Backpressure (NOT DONE)

*Currently, `QUEUE_CAPACITY` is a global limit across all three priority queues. Two spammy users can fill this instantly, causing 429 "Server Busy" errors for the other 998 users.*

**The Solution:**
- Remove the global queue length check in `app.py`.
- Check the tenant's isolated pending count: `get("stats:pending:{tenant_id}")`.
- If a single tenant has >50 pending tasks, they get a 429 error. Everyone else can continue using the queue freely. This guarantees fairness.

## 3. ✅ Env-Configurable Rate Limits (DONE — superseded design)

*Earlier versions hard-limited users to 10 req/min; later a benchmark value (10,000/min) leaked into source while `/metrics` reported 100.*

**Now:**
- `RATE_LIMIT_PER_MINUTE` (env, default 100) is the single source of truth — the enforcer and the `/metrics` reporter read the same constant, so they cannot drift.
- Because idempotency dedup happens *before* rate counting, client retries don't burn quota.
- Per-tenant backpressure (§2) remains the fairness mechanism for abuse beyond the rate limit.

## 4. 🚧 Horizontal Worker Scaling (MANUAL TODAY)

*A single Python instance with 4 CPU process workers will choke on 1,000 users.*

**The good news — already true today:** because Redis acts as a centralized broker with atomic `RPOPLPUSH` handoff, heartbeats, a zombie sweeper, and fencing tokens, you can run as many workers as you want **without changing any Python code** — duplicate or dead workers are already handled.

**Remaining work** — make it one command in `docker-compose.yml`:
```yaml
worker:
  build: .
  deploy:
    replicas: 5 # Spins up 5 worker instances (20 CPU processes total)
```

## 5. ✅ API-Key Tenancy (DONE)

Tenants are no longer tracked by IP address (which collapsed everyone behind NAT into one identity). Every request carries `Authorization: Bearer <API_KEY>`; the key maps to a `tenant_id` in Redis, and all state — stats, DLQ, metrics, task results — is namespaced by tenant with fail-closed reads.

## 6. 🚧 Redis High Availability (NOT DONE — the current SPOF)

**Today:** one Redis process holds every queue, counter, and result. If it dies, the platform is down and unpersisted state is lost.

**The path (no application rewrite needed — `redis-py` supports Sentinel natively):**
1. **Persistence first (AOF):** `appendonly yes` + `appendfsync everysec` → at most ~1s of accepted tasks lost on a crash.
2. **Sentinel + replica:** one master, one replica, three Sentinels; swap `redis.Redis(host=...)` for `sentinel.master_for("mymaster")` in `app.py`/`worker.py`. Everything else keeps working because the code only uses standard Redis commands.
3. **Honest residual risk:** Redis replication is asynchronous — a failover can drop the last few accepted tasks. Mitigation already in place: client idempotency keys deduplicate re-deliveries. For synchronous durability, the broker would need to move to RabbitMQ quorum queues or Kafka.
