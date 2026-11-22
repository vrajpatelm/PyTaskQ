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

PyTaskQ checks the policy (through `INFO`, which works even where `CONFIG` is disabled). When it isn't `noeviction`, every worker and the dashboard log an error at startup, `/health` reports it, and *Settings → Performance* shows it at the top.

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

**How much memory do jobs need?** Roughly the size of the job's JSON twice while it waits (its queue entry and its record), plus its result for `RESULT_TTL` (a day) after it finishes. A typical small job uses about 1–2 KB in total, so 400 MB holds a few hundred thousand of them. Large arguments or results change this a lot; `MAX_PAYLOAD_BYTES` and `MAX_RESULT_BYTES` (1 MB each) cap them.

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
- Open dashboards show "Redis unreachable" and continue updating on their own once Redis answers. *(Before 0.4.2 they froze until reloaded.)*

## When Redis loses its data

A Redis restarted **without its data volume**, a `FLUSHALL`, or eviction with the wrong policy leaves Redis empty or partly empty. Everything is gone: waiting jobs, history, dead letters, accounts and API tokens.

PyTaskQ keeps a marker key (`pytaskq:install`). If it disappears while PyTaskQ is running:

- the dashboard and every worker log an error: *"Redis lost its data … Was Redis restarted without its data volume, flushed, or is it evicting keys?"*;
- `/health` and *Settings → Performance* report it for a week;
- with sign-in on, the dashboard creates a **new setup code** and prints it in its log, so you can create the admin account again (`pytaskq users setup-code` prints it too). Old sessions and API tokens no longer work.

To restore the data from a backup instead, see [DEPLOYMENT.md](DEPLOYMENT.md#restoring).

## Passwords

A wrong or missing Redis password stops a worker at startup with exit code 1 and the message *"Redis refused this worker … Check REDIS_URL / REDIS_PASSWORD"*, so Docker shows it as failed instead of running and doing nothing. `pytaskq ping` tests the connection settings without starting anything.

## Checklist

- [ ] `maxmemory-policy` is `noeviction` (*Settings → Performance* shows no Redis warning).
- [ ] `maxmemory` is set, below the memory Redis can really have.
- [ ] `appendonly yes`, and Redis's `/data` is on a volume that survives restarts.
- [ ] Backups are taken and copied elsewhere ([DEPLOYMENT.md](DEPLOYMENT.md#backups)).
- [ ] Monitoring watches `/health` for `"status": "degraded"`, not only for its HTTP status.
