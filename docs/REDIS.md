# Running Redis for PyTaskQ

Redis holds everything PyTaskQ knows: waiting jobs, job records, schedules, dead letters, accounts and API tokens. Three settings decide whether Redis can lose that data without telling anyone. This guide explains them, and what PyTaskQ does when Redis is full, restarts or loses its data.

The compose files in this repo already set everything below. Read on if you run your own Redis, use a managed one (AWS ElastiCache, Azure Cache, Upstash, Redis Cloud…), or want to know why.

## The three settings that matter

| Setting | Use | Why |
|---|---|---|
| `maxmemory-policy` | **`noeviction`** | Any other policy deletes keys when Redis fills up, silently. |
| `maxmemory` | Below the memory Redis can really have | Otherwise the operating system kills Redis when memory runs out. |
| `appendonly` | **`yes`** (with `appendfsync everysec`) | Jobs survive a Redis restart. |

### `maxmemory-policy noeviction`

When Redis reaches `maxmemory`, the policy decides what happens:

- **`noeviction`**: Redis refuses new writes. PyTaskQ answers new jobs with `503 Redis is full` and keeps every job it already has. Workers keep taking and finishing jobs, which frees memory, and new jobs are accepted again.
- **Anything else** (`allkeys-lru`, `volatile-lru`, `allkeys-random`…): Redis **deletes keys** to make room. Which keys depends on the policy: a whole queue of waiting jobs, job records (so `cancel` and `status` stop working), idempotency keys (so retried requests create duplicate jobs), sessions (random sign-outs), and accounts. `.delay()` still reports success, and nothing is logged anywhere.

Managed Redis services often default to `volatile-lru` or `allkeys-lru`. Change it to `noeviction` in the provider's settings (it's usually called "eviction policy" or "maxmemory-policy").

PyTaskQ checks the policy (through `INFO`, which works even where `CONFIG` is disabled). When it isn't `noeviction`, every worker and the dashboard log an error at startup, `/health` reports it, and *Settings → Performance* shows it at the top. The workers' leader checks again every minute, so a policy switched on while everything runs (a plan change applied live, a failover to a differently configured replica) is logged too, and so is any key Redis evicted (*Redis evicted 4,576 key(s) since the last check…*).

### `maxmemory`

Set it below what Redis can really use, leaving room for the rest of the server:

| Server memory | Also runs the API and a worker? | `maxmemory` |
|---|---|---|
| 1 GB | yes | `300mb` |
| 2 GB | yes | `800mb` |
| 4 GB | yes | `1500mb` |
| Redis on its own machine | no | about 75% of its memory |

In the production compose file, set `REDIS_MAXMEMORY` in `.env` (default `400mb`). The development compose file uses `redis.conf` (`400mb`, under its 512 MB container limit).

Without `maxmemory`, Redis grows until the operating system's out-of-memory killer stops it. It then restarts from its append-only file and loses up to the last second of writes.

**How much memory do jobs need?** Roughly the size of the job's JSON twice while it waits (its queue entry and its record), plus its record and result after it finishes. A typical small job uses about 1–3 KB. Finished jobs are kept for `RESULT_TTL` (a day) but never more than `FINISHED_JOBS_KEPT` (50,000) of them, so with the defaults finished jobs settle at roughly 50–150 MB however busy you are. Add what's waiting: 400 MB holds a backlog of over a hundred thousand small jobs. Large arguments or results change this a lot; `MAX_PAYLOAD_BYTES` and `MAX_RESULT_BYTES` (1 MB each) cap them. *Settings → Performance* warns ahead of time when the kept jobs would fill Redis at the current rate.

### `appendonly yes`

Redis writes every change to an append-only file and replays it after a restart, so waiting jobs, records and accounts survive. With `appendfsync everysec` (the default in both compose files), a crash of the Redis machine can lose the **last second** of writes: jobs accepted in that second can disappear, and a job's "finished" status can be lost so that it runs again. An `Idempotency-Key` doesn't protect against this, because the key is in the same lost second. If that second matters, use `appendfsync always`, which makes every write wait for the disk.

## When Redis is full

With `noeviction` and `maxmemory` reached:

| What | What happens |
|---|---|
| New jobs through the API | `503 Redis is full` with `Retry-After`. Clients retry later. |
| New jobs from your app (`.delay()`) | `redis.exceptions.OutOfMemoryError` is raised. |
| Running workers | Keep taking, running and finishing jobs. That frees memory. |
| A worker starting now | Starts anyway, logs `Redis is out of memory (… used, maxmemory …)`, and drains the queue. |
| `/health` | `200` with `"status": "degraded"` and the memory use. |
| *Settings → Performance* | "Redis is full" (from 90%: "Redis is 92% full"), with what to change. |
| The dashboard's live badge | "Redis is full" while it can't read the numbers. |

To get out of it for good, raise `maxmemory`, add workers, or keep less: lower `RESULT_TTL` or `HISTORY_LIMIT`, or delete old dead letters.

*Before 0.4.2, a worker couldn't start while Redis was full, so nothing ever freed memory again.*

## When Redis restarts

A Redis restart (an upgrade, a crash, `docker compose restart redis`) is handled without anyone noticing:

- API requests made while Redis is down get `503` with `Retry-After`. The first request after Redis is back no longer fails: a connection that Redis closed is replaced and the request retried.
- Workers pause and continue. A job that finished while Redis was down keeps its result in memory, which is saved once Redis is back.
- The same holds while Redis answers but refuses writes for a while: `READONLY` (a failover in progress, or a client still pointed at the old primary), `NOREPLICAS` (`min-replicas-to-write` while a replica resyncs), `MISCONF` (a background save failed, often a full disk), `LOADING` (a restart reading its data), `BUSY` (another client's long Lua script). The outcome is saved once writes are allowed again, and the job isn't run twice. On `READONLY` the worker also reopens its connections, so a DNS-based failover reaches the new primary.
- Don't run long Lua scripts or `KEYS *` on PyTaskQ's Redis: while one runs past `busy-reply-threshold` (5 s) every other client gets `BUSY`.
- Open dashboards show "Redis unreachable" and continue updating on their own once Redis answers. *(Before 0.4.2 they froze until reloaded.)*

## When the network drops a connection

A NAT gateway, firewall, cloud load balancer or Docker's network can drop an idle connection without telling either side. Each PyTaskQ connection to Redis therefore has:

| Setting | Default | |
|---|---|---|
| `REDIS_SOCKET_TIMEOUT` | 15 | Seconds one command may take before the connection counts as dead and is reopened (at least 3) |
| `REDIS_CONNECT_TIMEOUT` | 5 | Seconds to open a connection |
| TCP keepalive | on | Probes an idle connection after 30 s, every 10 s |

Workers wait for new jobs with a blocking command that always ends below that timeout (5 s, or less when the timeout is set low), so an idle worker never mistakes its own wait for a failure. *(0.5.0 with redis-py 8 did: idle workers logged "Redis error: TimeoutError" every 10 s and picked up jobs up to 4 s late. With redis-py 5–7 a silently dropped connection could freeze a worker for good while its heartbeat said it was fine.)* As a last line of defence, a worker whose own connections get no answer for a minute while Redis is reachable reopens them, and stops (exit code 1, so it's restarted) if that doesn't help.

## When Redis loses its data

A Redis restarted **without its data volume**, a `FLUSHALL`, or eviction with the wrong policy leaves Redis empty or partly empty. Everything is gone: waiting jobs, history, dead letters, accounts and API tokens.

PyTaskQ keeps a marker key (`pytaskq:install`). If it disappears while PyTaskQ is running:

- the dashboard and every worker log an error: *"Redis lost its data … Was Redis restarted without its data volume, flushed, or is it evicting keys?"*;
- `/health` and *Settings → Performance* report it for a week;
- with sign-in on, the dashboard creates a **new setup code** and prints it in its log, so you can create the admin account again (`pytaskq users setup-code` prints it too). Old sessions and API tokens no longer work.

To restore the data from a backup instead, see [DEPLOYMENT.md](DEPLOYMENT.md#restoring).

## Sharing one Redis

Set `REDIS_KEY_PREFIX` to keep a PyTaskQ installation's keys apart from everything else in the same Redis database:

```bash
REDIS_KEY_PREFIX=shop        # keys become {shop}:queue:high, {shop}:task:…, {shop}:auth:…
```

- Use it to run several PyTaskQ installations (staging and production, two apps) on one Redis, or PyTaskQ next to your app's own keys. Each installation sees only its own queues, jobs, workers, accounts and dashboard.
- The braces make the prefix a Redis Cluster hash tag: every key of one installation lands in the same cluster slot, which PyTaskQ's multi-key scripts need. (PyTaskQ's client doesn't connect to Redis Cluster itself yet; this keeps the door open.)
- The dashboard, every worker and your app must use the same prefix, set before they start. A process with another prefix simply sees an empty installation.
- Without it (the default), keys keep the names earlier versions used. **Changing the prefix of an existing installation starts from an empty one**: the old keys stay where they were. Set it before you start, or move the data yourself (drain the queues first).
- A worker that finds PyTaskQ data under another prefix (or under none, when it has one) says so at startup and every few minutes: *"Found PyTaskQ data with no key prefix (5 job(s) waiting in its 'default' queue), but this process uses REDIS_KEY_PREFIX='myapp'"*. That's the sign of one process with a different `.env`.
- **Sharing a Redis with another app? Set a prefix.** Without one, PyTaskQ's key names (`dlq`, `stats:failed`, `queue:high`…) are generic, and another app's key of the same name and a different type would get in the way. PyTaskQ never loses a job over it (the step is refused before anything changes, and the log names the key), but jobs that need that key wait until it's fixed.

Separate Redis databases (`redis://…/1`) also keep installations apart, but managed Redis services and Redis Cluster often allow only database 0.

## High availability

A single Redis server is a single point of failure: while it's down, nothing can be queued or run, and if its disk is lost, so are the jobs. For a setup that survives losing a machine:

### Option 1: a managed Redis with automatic failover

AWS ElastiCache (with Multi-AZ), Azure Cache for Redis (Standard or Premium), Google Memorystore (Standard tier), Redis Cloud and others keep a replica and switch to it automatically, behind one address. Point `REDIS_URL` at that address. Remember to set the eviction policy to `noeviction`.

### Option 2: Redis Sentinel

Run a primary, at least one replica, and three Sentinels (on different machines), then tell PyTaskQ where the Sentinels are instead of where Redis is:

```bash
REDIS_SENTINELS=sentinel-1:26379,sentinel-2:26379,sentinel-3:26379
REDIS_SENTINEL_MASTER=mymaster          # the name in the Sentinels' "sentinel monitor" line
REDIS_PASSWORD=…                        # Redis's password (or put it in REDIS_URL; its host is then ignored)
# REDIS_SENTINEL_PASSWORD=…             # only if the Sentinels themselves need one
```

PyTaskQ asks the Sentinels which server is the primary and follows it when it changes. The Sentinels get short 2 s timeouts; the primary's connections get `REDIS_SOCKET_TIMEOUT` like any other. *(In 0.5.0 the primary got the Sentinels' 2 s timeout too, so idle workers timed out every 7 s.)* Tested with a real failover (the primary killed while jobs were being queued): the replica took over and PyTaskQ was queueing again within about 3 seconds, without losing a job that had been confirmed. During those seconds the API answers `503` with `Retry-After`, and workers wait and carry on.

### Waiting for a replica: `REDIS_WAIT_REPLICAS`

Redis copies writes to replicas a moment after it confirms them. If the primary dies in that moment, a job that `.delay()` already reported as queued can be missing on the replica that takes over. To close that gap:

```bash
REDIS_WAIT_REPLICAS=1          # after queueing a job, wait until 1 replica has it
REDIS_WAIT_TIMEOUT_MS=500      # …but at most this long
```

Each enqueue then waits (usually about one network round trip to the replica) until that many replicas have the job, and the reply includes `"replicas": 1`. If fewer confirm in time, the job is still queued, the reply says how many did, and a warning is logged. *Settings → Performance* warns when fewer replicas are connected than you asked for. It applies to queueing jobs, where losing one matters most; a job's later status changes aren't waited for.

### Workers

Run at least two workers, on different machines if possible: if one dies, the other takes over its jobs (after `WORKER_HEARTBEAT_TTL`, 30 s) and keeps the cron schedule running.

## Connections

Each worker holds about 6 connections when idle and a few more while busy; each API process up to `REDIS_MAX_CONNECTIONS` (200) under load, usually far fewer. `delay_sync()` keeps one small pool per process. Set Redis's `maxclients` (10,000 by default) with room to spare: when it's reached, workers say *"Redis has no free connections (maxclients reached)…"* and wait, and your app's `.delay()` fails.

## Memory per job

A waiting job takes about 1.5 KB plus about twice the size of its arguments (the job is kept in its queue and in its record). A finished job's record keeps its arguments and result for `RESULT_TTL`. With the shipped `maxmemory 400mb`, that's roughly 250,000 small waiting jobs, or about 1,700 with 100 KB of arguments each: keep arguments small and pass file locations instead.

## Passwords

A wrong or missing Redis password stops a worker at startup with exit code 1 and the message *"Redis refused this worker … Check REDIS_URL / REDIS_PASSWORD"*, so Docker shows it as failed instead of running and doing nothing. `pytaskq ping` tests the connection settings without starting anything.

## Checklist

- [ ] `maxmemory-policy` is `noeviction` (*Settings → Performance* shows no Redis warning).
- [ ] `maxmemory` is set, below the memory Redis can really have.
- [ ] `appendonly yes`, and Redis's `/data` is on a volume that survives restarts.
- [ ] Backups are taken and copied elsewhere ([DEPLOYMENT.md](DEPLOYMENT.md#backups)).
- [ ] Monitoring watches `/health` for `"status": "degraded"`, not only for its HTTP status.
- [ ] For production that must survive losing a machine: a replica with automatic failover (managed Redis or Sentinel), and two or more workers.
- [ ] If several installations share one Redis (or PyTaskQ shares it with another app), each has its own `REDIS_KEY_PREFIX`.
- [ ] No long Lua scripts or `KEYS *` run on it.
