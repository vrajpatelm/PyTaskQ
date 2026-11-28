# Upgrading

## From 0.6.1 to 0.7.0

0.7.0 fixes everything four break tests of 0.6.x found: most importantly, one job that crashes its worker can no longer take down every worker in turn, temporary Redis refusals no longer run jobs twice, and one backed-up queue no longer freezes every other queue's scheduled jobs. Waiting jobs keep working; nothing needs converting. Upgrade the dashboard and every worker together.

What behaves differently:

- **A job whose worker dies while running it 3 times** (`MAX_RECOVERIES`) goes to dead letters instead of being handed on forever. Set `MAX_RECOVERIES=0` for the old behaviour. A clean stop or a deploy doesn't count; a `kill -9` or an out-of-memory kill of a worker mid-job does.
- **`app.cancel()`** returns `"cancelled"` (it was waiting) or `"stopping"` (it was running) instead of `True`, or `False` as before. `if await app.cancel(id):` works unchanged; `== True` / `is True` doesn't. A running job asked to stop now always ends **Cancelled**, even if its code ran to the end meanwhile.
- **Queueing refuses what wouldn't arrive intact**: a `task_id` that isn't text of 1–200 characters without line breaks, dict keys that aren't text, values like `datetime`, `set`, `bytes` or `Decimal`, `args` given as a string, an `on_success_url` that isn't a public http(s) URL. Each raises `TypeError`/`ValueError` naming what's wrong, and nothing is queued. Before, they were queued and changed or failed later.
- **Task names** must be 1–200 letters, digits, `_`, `.`, `:` or `-`, starting with a letter or `_` (checked when the module is imported).
- **A job for a task no running worker has** waits up to `UNKNOWN_TASK_WAIT` (10 minutes) instead of failing at once. Set it to `0` for the old behaviour.
- **Settings that can't work stop the process at startup**: `RESULT_TTL` or `IDEMPOTENCY_TTL` below 1, `REDIS_MAX_CONNECTIONS` below 4, negative limits, a non-number in `WORKER_IO_WORKERS`/`WORKER_CPU_WORKERS`.
- **`idempotency_key=""`** raises `ValueError` (it was silently ignored).
- **SIGHUP stops a worker cleanly** (it used to kill it at once). A second Ctrl+C skips the grace period.
- **The retry of an IO job that timed out** waits until the old attempt's thread returns.
- **Async tasks that catch `CancelledError`** are left running in the background after `CANCEL_GRACE` (5 s) instead of holding their slot forever.
- **Rolling upgrade**: 0.6.1 and 0.7.0 workers can run side by side for the few minutes it takes. Recovery counting and the new checks only happen on 0.7.0 workers.

New: `Unrecoverable`, `with_job()`, cron `overlap=False`, settings `MAX_RECOVERIES`, `CANCEL_GRACE`, `LOOP_BLOCK_GIVE_UP`, `UNKNOWN_TASK_WAIT`.

## From 0.5.0 to 0.6.0

0.6.0 fixes every problem found by two break tests of 0.5.0 (one of them slowed every new install: idle workers on redis-py 8 picked up jobs up to 4 s late and logged a false error every 10 s) and adds workflows. Waiting jobs keep working; nothing needs converting. Upgrade the dashboard and every worker together.

What behaves differently:

- **Calling a task with arguments it can't take** (`task.delay(1, 2, 3)` when it takes one) raises `TypeError` in your code at once. Before, the job was queued and dead-lettered later.
- **`delay(x, countdown=30)`** now waits 30 s (it used to pass `countdown` to your function). Same for `task_id=`, and `delay_sync()` accepts both. If your function has its own parameter named `countdown` or `task_id`, it still gets it.
- **`app.wait(id)`** raises `LookupError` at once for an id with no record, instead of waiting the whole timeout.
- **`sys.exit()` / `KeyboardInterrupt` inside a task** fail the job (`TaskExited`) instead of stopping the worker.
- **`GET /dlq`** returns 100 dead letters at a time (`offset`, `limit`, `total`), and job lists send compact records (long errors cut, big results/arguments left out with their size). `GET /task/{id}` still has everything.
- **Error texts** are cut to `MAX_ERROR_BYTES` (4 KB).
- **`max_concurrency`** jobs wait in line and start the moment a place frees up (before: about one start a second, in random order). **`rate_limit`** never lets two starts closer than the limit, even after a pause or a replay.
- **Cron runs for a paused queue are skipped**, not piled up.
- **Replaying a dead letter** is refused while a job with the same id is waiting or running.
- New settings: `REDIS_SOCKET_TIMEOUT` (15), `REDIS_CONNECT_TIMEOUT` (5), `MAX_ERROR_BYTES` (4096). The defaults are right for almost everyone.

New: [workflows](WORKFLOWS.md) (`chain`, `group`, a Workflows tab).

## From 0.3.1 to 0.5.0

The last release on PyPI before 0.5.0 was **0.3.1**. Versions 0.4.0 to 0.4.2 were tagged on GitHub but never published, so this section goes straight from 0.3.1 to 0.5.0. If you run a 0.4.x build from GitHub, skip what you already have; the [CHANGELOG](../CHANGELOG.md) lists every version. Coming from 0.3.1, read this section, then the one above.

Jobs already waiting in Redis keep working: no data needs converting, and no key is renamed.

## How to upgrade

Upgrade the dashboard and **every** worker to the same version, with the same `.env`.

```bash
pip install -U pytaskq            # 0.7.0
```

or with Docker, set `PYTASKQ_VERSION=0.7.0` in `deploy/.env` (or `FROM ghcr.io/vrajpatelm/pytaskq:0.7` for your worker image), then:

```bash
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

Old and new workers can run side by side for the few minutes a rolling upgrade takes. *Settings → Performance* says when versions or shared settings differ, and the Workers tab shows each worker's version.

Take a backup first ([DEPLOYMENT.md](DEPLOYMENT.md#backups)).

## Check these before upgrading

### Redis settings (important)

PyTaskQ now checks how Redis is set up and warns loudly when it's unsafe. Before upgrading, make sure your Redis has:

- `maxmemory-policy noeviction`. Any other policy deletes jobs and accounts silently when Redis fills up. Managed Redis services often default to `volatile-lru` or `allkeys-lru`: change it.
- `maxmemory` set below the memory Redis can really have.
- `appendonly yes`.

The shipped compose files do all three (`REDIS_MAXMEMORY` in `deploy/.env`, default `400mb`). [REDIS.md](REDIS.md) explains why.

### Cron schedules now run in UTC

Before, schedules used each worker's local time. Now they're read in UTC unless the task sets `tz=` or you set `CRON_TIMEZONE`. Servers in Docker were already on UTC. If yours weren't, add `tz="Europe/London"` (your zone) to the task.

After an outage, the latest missed run of each scheduled task now runs once when workers are back. Set `catch_up=False` on a task (or `CRON_CATCH_UP=false` for all) if a late run would be wrong.

### Priorities are weighted, not strict

`high` jobs are still taken first most of the time, but `default` and `low` jobs now get a share even while high ones keep arriving (6 : 3 : 1 by default), so they can't wait forever. Set `PRIORITY_WEIGHTS=strict` for the old behaviour.

### Finished jobs are capped

At most `FINISHED_JOBS_KEPT` (50,000) finished jobs are kept, the oldest go first, besides the existing `RESULT_TTL` (a day). This keeps Redis memory flat at any volume. If you look up results older than the newest 50,000 jobs, raise it (each job takes about 1–3 KB).

### The API is stricter

| Now | Before |
|---|---|
| A job whose arguments don't fit the task's parameters gets `422` | Queued, then failed on every retry |
| Jobs over `MAX_PAYLOAD_BYTES` (1 MB) get `413`; `.delay()` raises `ValueError` | Accepted |
| Task names starting with `_` can't be queued through the API (`403`) | Allowed |
| `"queue"` must name a queue that exists (`422` otherwise) | Any name, and the job waited forever |
| `QUEUE_CAPACITY` applies to each queue, `0` means no limit | One limit for everything; `0` refused every job |
| `/metrics` has `queues` and `fullest_queue`, not `rate_limit_used` / `rate_limit_max` | — |
| `POST /task/{id}/cancel` on a running job returns `202` and stops it | `409` |

### Your code

- `apply_async(task_id=...)` with the id of a job that is still waiting or running raises `TaskIdInUse` (a `ValueError`). A finished job's id can be reused.
- Results over `MAX_RESULT_BYTES` (1 MB) aren't stored; the job still succeeds and its record says why (`result_dropped`).
- A job's record no longer expires while it waits or runs; `RESULT_TTL` counts from when it finishes.
- Workers that stop wait up to `WORKER_SHUTDOWN_GRACE` (30 s) for running jobs, then put unfinished ones back on their queue. In Docker, give workers `stop_grace_period: 40s` (the shipped compose files do).

### Docker Compose files you wrote yourself

Copy these from the shipped files:

- workers: `stop_grace_period: 40s` and `WORKER_NAME={hostname}` (a restarted worker puts back its own unfinished jobs at once);
- Redis: `--maxmemory ${REDIS_MAXMEMORY:-400mb} --maxmemory-policy noeviction --appendonly yes`;
- web and worker: `ulimits: nofile: 65536`.

## New things worth turning on

None of these are required.

- **Queues** for different kinds of work, each with its own workers: `@app.task(queue="images")`, `pytaskq worker --queues images`. [QUEUES.md](QUEUES.md)
- **`WORKER_NAME={hostname}`** for instant recovery after a worker restarts.
- **`progress()`** in long tasks, and **`cancelled()`** in long IO tasks. [TASKS.md](TASKS.md)
- **`max_concurrency` / `rate_limit`** for tasks that call something with a limit. [TASKS.md](TASKS.md#limiting-how-many-run-and-how-fast)
- **Idempotency keys** on anything a client might send twice.
- **Prometheus**: scrape `/metrics/prometheus` with a viewer API token. [DEPLOYMENT.md](DEPLOYMENT.md#monitoring)
- **High availability**: Redis Sentinel (`REDIS_SENTINELS`) and `REDIS_WAIT_REPLICAS`. [REDIS.md](REDIS.md#high-availability)
- **`REDIS_KEY_PREFIX`** to share one Redis between several installations. [REDIS.md](REDIS.md#sharing-one-redis)
- **Tracing** with OpenTelemetry (`OTEL_EXPORTER_OTLP_ENDPOINT`).

## After upgrading

Open *Settings → Performance*. It should show no warnings about Redis, versions or settings. Then run a job and watch it go through.
