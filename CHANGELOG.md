# Changelog

All notable changes to PyTaskQ. Versions follow [semantic versioning](https://semver.org/).

## 0.6.1 — 2026-11-25

### Fixed
- A job could stay stuck in a worker's processing list, never running, until that worker stopped: when Redis moved a job to a worker but the reply was lost (Redis restart, failover, dropped connection), the worker never received it, and the sweeper only recovers dead workers' lists. Each worker now checks its own list every 30 s and puts back jobs it never received.
- A cancel request that came too late to stop a run (the job succeeded or was dead-lettered first) was kept for a week and silently cancelled the job when it was replayed from dead letters, or when its id was reused. The request now ends with its run; a cancelled job's retries still never run.
- `rate_limit` above about one start a second ran at about one a second (`"5/s"` gave 1/s). Waiting jobs are now released when their turn comes rather than on a one-second tick, and a job whose turn is due but whose previous start ran a few milliseconds late keeps its place in line.
- `rate_limit` starts could be closer than the limit (0.17 s apart at `"5/s"`): the worker sent its own idea of the current time, which could be stale by the time Redis ran the check, or skewed between machines. The check now uses Redis's clock.
- When a worker was killed (`kill -9`, the out-of-memory killer), its CPU processes kept running forever, each holding a full Python interpreter's memory: a forked process inherits the pipes' other ends, so it never saw its own pipe close. CPU processes now exit within a second of their worker's death.
- The workflow sweep only looked at the 500 newest workflows of the last 7 days, so an older stuck workflow stayed Running forever. Running workflows are now tracked in their own sorted set (`pytaskq:workflows:running`); the sweep reads only the ones that may be stuck, in one pipelined round trip. Workflows started by 0.6.0 are added on the first sweep after upgrading.

### Fixed (found by type checking)
- `app.workflow(workflow_id=…)` with an id already in use raised `TypeError` instead of an "already in use" error. It now raises `WorkflowIdInUse` (a `TaskIdInUse`).
- Changing a user's role while the user was being deleted returned nothing instead of a 404.

### Changed
- The built dashboard (`pytaskq/static/`) is no longer committed. CI, the release workflow and the Dockerfile build it from `frontend/`, so the wheel on PyPI and the Docker image still include it. From a source checkout, run `cd frontend && npm ci && npm run build` once; without it the API works and `/` explains how to build the dashboard. Dependency updates for the dashboard no longer need a rebuilt bundle in the same pull request.
- The package is typed (`py.typed`) and checked with mypy in CI. Coverage is measured in CI and must stay at 84% or more. CI also runs the suite on Windows (required) and macOS (experimental).
- Classifiers say Linux and Windows instead of "OS Independent".
- The worker's Lua scripts moved to `pytaskq/worker_scripts.py` (still importable from `pytaskq.worker`).
- Added `SECURITY.md`, `CODE_OF_CONDUCT.md`, issue and pull request templates, Dependabot and a pre-commit configuration. `[dev]` now includes mypy, pytest-cov and pre-commit.
- Code comments trimmed: release history and issue references removed, contracts kept.
- Tests that start real workers keep the existing `PYTHONPATH` instead of replacing it. CI also runs on Python 3.14.

## 0.6.0 — 2026-11-24

Fixes everything two break tests of 0.5.0 found (#3: D1–D11, #4: E1–E11), and adds workflows. New guide: `docs/WORKFLOWS.md`; `docs/UPGRADING.md` has a 0.5.0 → 0.6.0 section.

### Upgrading from 0.5.0
- The SDK checks a job's arguments against the task's signature and raises `TypeError` at once (`delay()`, `apply_async()`, `delay_sync()`), like the API's `422`.
- `delay()` / `delay_sync()` accept `countdown=` and `task_id=` as options (before, `countdown` was passed to the task as an argument and the job ran at once).
- `app.wait()` raises `LookupError` for an unknown id instead of waiting out the timeout.
- `GET /dlq` is paged (`offset`, `limit` ≤ 500, default 100; `total`). Job lists (`/tasks`, `/tasks/history`, `/dlq`) send compact records; `GET /task/{id}` has everything.
- Error texts are cut to `MAX_ERROR_BYTES` (4 KB).
- Cron ticks for a paused queue are skipped instead of piling up.

### Added
- **Workflows**: `chain(...)`, `group(...)`, `task.s()` / `task.si()`; results passed to the next stage (a list after a group); `on_failure="stop"|"continue"`; retry failed steps and the workflow resumes; cancel; `app.workflow()`, `workflow_status()`, `wait_workflow()`, `cancel_workflow()`, `retry_workflow()`; `GET/POST /workflows…`; a Workflows tab with each stage and step. Stage changes are atomic in Redis (no result backend, no polling); lost stages are re-queued by the leader. Steps run on their task's queue and priority; a stage's results are stored once and read by the next stage's steps; `Idempotency-Key` on `POST /workflows` and `app.workflow(idempotency_key=)`.
- Break test #5 (workflows together with the older features), all fixed before release: a later stage ignored its task's queue and priority (W1); a Failed workflow whose other steps finished after the failure never resumed after a retry (W2); retrying a step of an `on_failure="continue"` workflow was accepted but ignored, now refused (W3); cancelling one step left its siblings running (W4); a step queued while its workflow was being cancelled still ran (W5); the recovery sweep re-ran a finished step whose record was trimmed (W6) and never counted a step whose worker stopped before telling the workflow (W7); a bad priority or queue left a workflow stuck in Running (W8, W12); a group's results were copied into every job of the next stage (W9: +126 MB for 5 MB of results); a failed or step-cancelled workflow never expired.
- Settings `REDIS_SOCKET_TIMEOUT` (15), `REDIS_CONNECT_TIMEOUT` (5), `MAX_ERROR_BYTES` (4096).
- *Settings → Performance* warns when Redis has no `maxmemory`.

### Fixed (break test #4)
- **E1 (critical)** `sys.exit()` or `KeyboardInterrupt` inside any task stopped the whole worker, and the job then stopped every worker it was handed to. Now the job fails with `TaskExited`.
- **E2** A result or error containing text that isn't valid UTF-8 left the job stuck in *Processing*, running again on every restart. Text is cleaned; any outcome that can't be saved becomes a dead letter with the reason.
- **E3** An `async def` behind an ordinary decorator never ran (marked Success with `"<coroutine object…>"`). It now runs as an async task; returned coroutines are awaited and generators read.
- **E4** After a lost Redis reply, the client's automatic re-send made the caller see `TaskIdInUse` for a queued job (or run it twice), and a failure could become two dead letters. The scripts now recognise their own repeat.
- **E5** With Redis Sentinel, idle workers timed out every 7 s (the primary got Sentinel's 2 s timeout).
- **E6** Two containers with the same `WORKER_NAME`, hostname and pid 1: the second took the first one's running job. Restarts are now recognised by a boot id.
- **E7** Replaying a dead letter while the same id was running started a second copy. Refused now (`409`); replays get a new fence token; "Replay all" queues a doubly dead-lettered job once.
- **E8** Error messages had no size limit (20 MB errors → 60 MB list responses). Cut to 4 KB; lists send previews.
- **E9** `GET /dlq` returned everything (22 MB for 10,000). Paged.
- **E10** SDK argument mistakes reached the worker; `countdown` in `delay()` became an argument.
- **E11** `NaN`/`Infinity` results were stored as invalid JSON. Stored as `null`, with a warning (also for values stored as their text).

### Fixed (break test #3)
- **D1** With redis-py 8 (every new install), idle workers got a `TimeoutError` every 10 s and picked up jobs up to 4 s late. Connections now have an explicit 15 s read timeout above the 5 s idle wait, and a timed-out wait counts as "no work".
- **D2** With redis-py 5–7, a connection dropped silently by a NAT/firewall froze the worker forever while its heartbeat looked fine. Timeouts and TCP keepalive on every connection; the heartbeat thread also checks that the worker's own connections answer, reopens them, and stops the worker if that doesn't help.
- **D3** `max_concurrency` was exceeded when IO jobs timed out or were cancelled (3 running with a limit of 1). Leases are per attempt and kept until the thread really returns.
- **D4** `rate_limit` let jobs through in bursts after a pause, a busy period or a DLQ replay. A turn is re-checked right before the start.
- **D5** A cancelled job could start again after its worker restarted. The cancel request is checked right before every start.
- **D6** `max_concurrency` managed about one start per second, in random order. A finishing job now starts the one that waited longest at once: 20 × 0.1 s jobs take 2 s instead of 70.
- **D7** History search read up to 5,000 whole records (up to ~10 GB). It reads small fields only, and caps what a text search reads.
- **D8** Cancelled rate-limited jobs kept their turns (a new job waited for all of them). Cancelling gives the turn back.
- **D9** `/tasks/history` answered `500` for `since=nan` and huge cursors. `422`.
- **D10** Pausing a queue didn't pause cron; ticks piled up and all ran on resume. They're skipped.
- **D11** `progress(10**400)` failed the job; `wait()` on an unknown id waited the whole timeout; no warning for a Redis without `maxmemory`; outdated advice text; a double period in a log line.

## 0.5.0 — 2026-11-22

The first release on PyPI since 0.3.1: it includes everything from 0.4.0, 0.4.1 and 0.4.2 below, which were tagged on GitHub but never published. New in 0.5.0: a review of PyTaskQ from its foundations up (how jobs are stored, picked up, scheduled and kept), followed by the features that review called for. New guides: `docs/TASKS.md` (writing tasks), `docs/GUARANTEES.md` (what PyTaskQ promises and what it doesn't) and `docs/UPGRADING.md` (from 0.3.1).

### Upgrading from 0.4.2
- **Priorities are weighted**: out of every 10 jobs a worker takes while all priorities have work, about 6 are high, 3 default and 1 low (`PRIORITY_WEIGHTS=6,3,1`). Before, high jobs always went first and a steady stream of them kept low ones waiting forever. `PRIORITY_WEIGHTS=strict` restores that.
- **At most `FINISHED_JOBS_KEPT` (50,000) finished jobs are kept**, the oldest going first, besides `RESULT_TTL`. Raise it if you look up older results.
- **Missed cron runs now run once, late.** If no worker was running at a scheduled time, the latest missed tick runs when a worker is back. Set `catch_up=False` on a task (or `CRON_CATCH_UP=false`) to skip them as before.
- **`POST /task/{id}/cancel` on a running job** now stops it (`202`, `"stopping": true`) instead of answering `409`. `app.cancel()` returns `True` for it.
- Upgrade the dashboard and every worker together. *Settings → Performance* now says when versions or shared settings differ.
- Coming from 0.3.1? Read `docs/UPGRADING.md`.

### Added
- **Progress**: `pytaskq.progress(percent, message)` inside any task (CPU tasks too); shown as a bar in the dashboard and returned by `GET /task/{id}`. Written about once a second however often it's called.
- **Cancel running jobs** from the dashboard (*Stop job*), the API or `app.cancel()`: async tasks are cancelled, CPU tasks' processes stopped, and IO tasks told through `pytaskq.cancelled()` with their result ignored. A job a worker had taken but not started is cancelled without running.
- **`max_concurrency=` and `rate_limit=`** on `@app.task`, enforced across all workers: at most N running at once, or starts spread evenly (`"10/s"`, `"100/m"`, `"1000/h"`). Waiting never uses a retry, and the dashboard shows why a job waits.
- **Pause and resume queues** (dashboard button, `POST /queues/{name}/pause|resume`, `app.pause_queue()`). Takes effect at once; running jobs finish, new jobs wait. `GET /queues` lists queues with their state.
- **History**: search every kept finished job by text (id, arguments, result, error), outcome, task, queue and time, in a new dashboard tab and `GET /tasks/history` (cursor-paged).
- **Prometheus**: `GET /metrics/prometheus` (queue lengths, running, scheduled, dead letters, workers, succeeded and failed totals, missed cron runs, refusals, Redis memory).
- **Redis Sentinel**: `REDIS_SENTINELS`, `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_PASSWORD`. Tested with a real failover: queueing resumed within about 3 seconds with no confirmed job lost.
- **`REDIS_WAIT_REPLICAS`** / `REDIS_WAIT_TIMEOUT_MS`: a queued job is confirmed only once that many replicas have it; the reply says how many did.
- **`REDIS_KEY_PREFIX`**: every key under `{prefix}:`, so several installations can share one Redis; the braces make the layout Redis Cluster-compatible.
- **Fleet check**: workers advertise their version and shared settings; the dashboard warns when they differ, and the Workers tab shows each version.
- **Cron catch-up and missed-run reporting**: missed ticks are logged, counted (*Settings → Performance*, Prometheus) and the latest one runs once (`catch_up=` per task, `CRON_CATCH_UP`).
- **Memory forecast** in *Settings → Performance*: warns when the kept finished jobs would fill Redis at the current rate, before anything is refused.
- `pytaskq.current_task_id()`, `app.pause_queue()`, `app.resume_queue()`.
- New settings: `FINISHED_JOBS_KEPT`, `PRIORITY_WEIGHTS`, `CRON_CATCH_UP`, `REDIS_KEY_PREFIX`, `REDIS_SENTINELS`, `REDIS_SENTINEL_MASTER`, `REDIS_SENTINEL_PASSWORD`, `REDIS_WAIT_REPLICAS`, `REDIS_WAIT_TIMEOUT_MS`.
- Docs: `TASKS.md`, `GUARANTEES.md`, `UPGRADING.md`; high availability, sharing a Redis, monitoring, priorities, pausing and missed runs in the existing guides.

### Changed
- **Instant pickup**: queueing a job wakes an idle worker through a wake-up token, so jobs start within 1–3 ms instead of up to a second (about 475 ms on average), and an idle worker sends about 10–12 Redis commands a second instead of 15–65 (with 20 queues).
- **Redis memory stays flat**: finished jobs are capped by count (above). With 100 jobs a second, memory used to grow to about 20 GB a day.
- Cron tick locks are kept 3 hours instead of 30 days (an every-minute task left 43,200 keys behind).
- `queue.wait()` checks every 20 ms at first, then less often, so quick jobs return quickly.
- The Queues row is shown to operators even with a single queue, so it can be paused.

## 0.4.2 — 2026-11-22

Fixes from a second break test, on what happens when Redis itself misbehaves: full, set to evict keys, restarted, frozen, wiped, or given the wrong password. New guide: `docs/REDIS.md` (the three Redis settings that matter, and what PyTaskQ does when Redis is full, restarts or loses its data).

### Upgrading from 0.4.1
- **Set Redis to `maxmemory-policy noeviction` with a `maxmemory`.** Both compose files and `redis.conf` now do (`REDIS_MAXMEMORY`, default `400mb`, in the production `.env`). Managed Redis services often default to an eviction policy: change it. PyTaskQ now warns when it isn't `noeviction`.
- **Through the API, `"queue"` must name an existing queue** (one a worker serves or a task lives on). New names get `422` with the list of known queues. Your app's `.delay(queue=...)` can still create queues; at most `MAX_QUEUES` (100) can exist.
- **`apply_async(task_id=...)` with the id of a job that is still waiting or running raises `TaskIdInUse`** (a `ValueError`) instead of corrupting both jobs.
- `/health` returns `"status": "degraded"` with `warnings` (still HTTP 200) when Redis is nearly full, set to evict, or lost its data; `503` (`"status": "down"`) only when Redis can't be reached.

### Fixed
- **A worker couldn't start while Redis was full**, so nothing ever took jobs to free memory (0.4.1 even crashed at startup). Workers now start and drain the queue; taking and finishing jobs, heartbeats and leader election work when Redis is full, while new jobs are refused.
- **Eviction silently deleted queues, job records, idempotency keys and accounts.** The shipped configs use `noeviction` with a memory limit; workers and the dashboard log an error, and `/health` and *Settings → Performance* show it, when the policy is anything else or Redis is over 90% full.
- **Open dashboards froze forever after a Redis restart** (the socket stayed open, updates had stopped). They now show "Redis unreachable" and resume by themselves; if updates ever stop, the socket is closed so the browser reconnects.
- **Reusing a `task_id` broke the job**: its result was thrown away as stale and it stayed "Processing" for a day, and two schedules with the same id silently merged into one. A finished job's id is now reused cleanly; an active one is refused.
- **Queue names piled up forever** (5,000 names made every dashboard refresh 48× slower). Unused names are forgotten, the API refuses unknown ones, and `MAX_QUEUES` caps them.
- **After Redis lost its data, the dashboard asked for a setup code that didn't exist**, and nothing said why. PyTaskQ notices the loss (a marker key), logs it, shows it in `/health` and *Settings → Performance*, and prints a new setup code.
- A frozen or full Redis gave `500`; now `503` with `Retry-After` (`"Redis is full"` when it's full), also for timeouts and a read-only replica during failover.
- The first request after a Redis restart failed with `503`; a connection Redis closed is now replaced and the request retried (connection errors only, never timeouts, since a timed-out command may have run).
- A worker with a wrong Redis password retried forever; it now stops with exit code 1 and says to check `REDIS_URL` / `REDIS_PASSWORD`. Stopping a worker during startup no longer logs "started and ready".
- `redis.conf` claimed idempotency keys made a lost second of AOF data harmless; they don't. The comment now explains the real trade-off.

### Added
- `docs/REDIS.md`.
- Settings: `MAX_QUEUES` (100), `REDIS_MAXMEMORY` (compose, 400mb).
- `/health` reports Redis memory use and policy; *Settings → Performance* reports Redis memory problems, refusals because Redis was full, and data loss.

## 0.4.1 — 2026-11-21

Reliability fixes from a break test of 0.4.0: crashed and hung tasks, Redis outages mid-job, frozen event loops, invalid jobs and request floods, on a real Redis with real worker processes. Every fix has a regression test, and the experiments were re-run. New guide: `docs/WORKERS.md` (how tasks run, timeouts per task type, what happens when things go wrong).

### Upgrading from 0.4.0
- **The API now checks a job's arguments** against the parameters the workers advertise, and answers `422` for a missing required argument, an unknown keyword or too many positional ones (before, the job failed on every retry in the worker).
- **Jobs over 1 MB are refused** (`MAX_PAYLOAD_BYTES`, `0` = no limit): `413` from the API, `ValueError` from `.delay()`.
- **Task names starting with `_` can't be queued through the API** (`403`). They're PyTaskQ's internal tasks.
- A job's record no longer expires while it waits or runs; `RESULT_TTL` now counts from when it finishes.
- A job recovered from a crashed worker keeps its priority (it used to be moved to `high`) and goes to the front of its queue.
- Stopping a worker gives running jobs `WORKER_SHUTDOWN_GRACE` (30 s), then puts any still running back on their queue. Before, it waited forever. The compose files set `stop_grace_period: 40s` and `WORKER_NAME={hostname}` for workers.
- `QUEUE_CAPACITY=0` now means "no limit" (it used to refuse every API job).

### Fixed
- **A crashed CPU task process froze the whole worker** (the out-of-memory killer, a C extension crash, `os._exit`): it stopped heartbeating and ignored SIGTERM. Now only that job fails (`ProcessCrashed`, retried normally) and a fresh process takes its place. If processes can't be started at all, the worker exits with code 1 so it gets restarted.
- **A timed-out task kept its slot, so every later job timed out without running.** CPU tasks now run one per process, and a timed-out one's process is stopped and replaced. A timed-out IO thread (which Python can't stop) keeps its slot until it returns, so later jobs wait for a thread instead of timing out behind it; when every IO thread is stuck, the worker leaves IO jobs for other workers.
- **The timeout included time spent waiting for a free process or thread**, so bursts of CPU jobs were dead-lettered while waiting. The timeout, `started_at` and the *Processing* status now start when the function does.
- **Shutdown hung forever on a stuck CPU task.**
- **A Redis outage as a job finished left it "Processing" forever**, and a job running at a clean stop was orphaned. Saving an outcome and removing the job from the worker's list are now one atomic step, retried until Redis is back; a stopping worker puts unfinished jobs back; the leader also recovers jobs listed by workers that have no heartbeat.
- **A worker whose event loop was blocked looked dead, so its job ran twice at once.** The heartbeat now runs in its own thread with its own Redis connection, and a watchdog logs which async task is blocking the loop (also shown on the Workers tab and in *Settings → Performance*).
- **The sweeper could queue a job that had just finished again**, overwriting Success. Recovery is now one Lua script that does nothing if the job is no longer listed or already finished.
- **A fast cron job could end up "Queued" forever** (its record was written after the push). The tick lock, the record and the push are now one script.
- **`QUEUE_CAPACITY` could be overshot** (limit 10, 29 accepted under load). The check and the push are now one script: exact under any load. Scheduled jobs through the API respect it too, and due scheduled jobs and retries move onto a queue only while it has room (in batches of 500, with one log line per pass instead of one per job: 10,000 jobs in 0.2 s instead of 50,000 in 28 s).
- **Each job's arguments were stored three times.** Now once (`args`/`kwargs` are read from the stored job). Dead letters are capped at `DLQ_LIMIT` (10,000).
- **Invalid jobs vanished silently**, and all of them overwrote one `task:Unknown` record. They now become "(invalid job)" dead letters with the text that was sent. A non-object entry in the delayed set no longer logs an error every second forever. Stored jobs are validated (priority, non-negative retry count and so on).
- **A job waiting longer than `RESULT_TTL` lost its record** (it couldn't be cancelled but still ran).
- **Recovery on worker start never ran.** With `WORKER_NAME` (or `pytaskq worker --name`; `{hostname}` allowed), a restarted worker puts back its unfinished jobs at once instead of after about 40 seconds. Two live workers can't share a name.
- A 2 MB task name was echoed back in the error; names are limited to 200 characters and long rejected values are shortened in `422` replies.
- The leader lock is taken and renewed in one step, so a leader can't renew another worker's lock.
- Heartbeats, retry times and schedules use Redis's clock, so a machine with a wrong clock no longer looks dead.
- An async task that overruns its timeout because it blocked the event loop is logged as such.
- A promoted retry or scheduled job shows *Queued* while it waits (it showed *RetryScheduled*).

### Added
- `docs/WORKERS.md`.
- Settings: `MAX_PAYLOAD_BYTES` (1 MB), `DLQ_LIMIT` (10,000), `WORKER_SHUTDOWN_GRACE` (30 s), `WORKER_HEARTBEAT_TTL` (30 s), `WORKER_NAME`; `pytaskq worker --name`.
- `/system/workers` reports `stuck_io`, `cpu_restarts` and `loop_blocked_seconds`; the Workers tab and *Settings → Performance* explain them.
- The dashboard's dead letters show what an invalid job contained, and offer only Delete for it.

## 0.4.0 — 2026-11-20

Named queues, cron time zones, and everything from the unreleased 0.3.2: faster under load, safer to retry, traceable end to end, and a set of bugs fixed after a close review. On a 2-core server with 500 simulated users, the "Redis unavailable" errors went from 14% of requests to none, and the typical response from 320 ms to 16 ms. Details in `docs/PERFORMANCE.md`.

### Upgrading from 0.3
- **Cron schedules now run in UTC** unless a task sets `tz=` or you set `CRON_TIMEZONE`. Before, they used each worker's local time. Servers in Docker were already on UTC.
- Jobs already waiting in Redis keep working: the `default` queue keeps its key names.
- `/metrics` lost `rate_limit_used` and `rate_limit_max` (the rate limit was never enforced), and gained `queues` and `fullest_queue`. `QUEUE_CAPACITY` now applies to each queue.

### Added
- **Named queues.** `@queue.task(queue="images")` or `PyTaskQ(default_queue=…)` puts tasks on their own queue, and only workers serving it run them. Workers serve their own tasks' queues by default, or `--queues a,b` / `WORKER_QUEUES`. Priorities work within each queue, and a busy queue can't starve another. Each queue has its own `QUEUE_CAPACITY`. A single job can be routed elsewhere (`delay(..., queue=…)`, `"queue"` in the API). `/tasks?queue=` filters, `/system/workers` lists each worker's queues, and `pytaskq tasks` shows each task's queue. New guide: `docs/QUEUES.md`.
- **Dashboard queues row**: waiting jobs and serving workers per queue (click one to filter the job list); queues with jobs but no worker are marked, and *Settings → Performance* says which worker to start. Job details show the queue.
- **Cron time zones**: `tz="Asia/Kolkata"` per task, `CRON_TIMEZONE` for the default. Bad names are rejected when the task is defined (or when the worker starts). When clocks go back, a job runs once; when they go forward, a skipped time runs at the jump.
- **`MAX_RESULT_BYTES`** (1 MB): larger results aren't stored. The job still succeeds, and `result_dropped` (also in webhooks and the dashboard) says why.
- A worker that takes a job whose task it doesn't have (two apps sharing a queue) hands it back for a worker that has it, for up to 10 minutes, without using a retry; *Settings → Performance* suggests giving each app its own queue.
- **Settings → Performance**: what the server turned away in the last 15 minutes, live load figures, and plain-language suggestions with the exact setting to copy into `.env`. Backed by `GET /system/performance`.
- **Rename API tokens** from Settings (`PATCH /auth/me/tokens/{id}`). The secret doesn't change.
- **Jobs list stays readable during bursts.** It holds still while your pointer is over it, you've scrolled down or you press Pause; new jobs wait behind a "new jobs" button, and statuses still update in place. Runs of the same task and status fold into one row you can expand ("Group repeated jobs").
- The Queued box shows "Nearly full" / "Full: new jobs are refused" with a link to the advice, without moving the page.
- `pytaskq dashboard --workers N` and the `WEB_WORKERS` setting to run several API processes.
- New settings: `REDIS_MAX_CONNECTIONS` (200), `REDIS_POOL_TIMEOUT` (5 s), `WEB_WORKERS` (1), `LIVE_UPDATES_PER_SECOND` (2).
- `docs/PERFORMANCE.md` and the load test in `loadtest/`.
- **Idempotency keys.** Send an `Idempotency-Key` header with `POST /task/enqueue` (or `delay(..., idempotency_key=…)` in the SDK) and a retried submission returns the original job with `"duplicate": true` instead of queueing a second one, even while the queue is full. Remembered for `IDEMPOTENCY_TTL` (a day).
- **End-to-end tracing.** With `OTEL_EXPORTER_OTLP_ENDPOINT` set, one OpenTelemetry trace follows each job: the API request (continuing a caller's `traceparent`), the enqueue, every attempt, and follow-up webhook jobs. `task.delay()` inside your own app's span joins your trace. Spans never contain URLs, headers, bodies or job arguments. `https://` collectors use TLS, and `OTEL_SERVICE_NAME` is respected.

### Changed
- The dashboard's Queued, Running, Scheduled and Dead-letter numbers are counted from Redis instead of hand-maintained counters, which drifted after crashes and could go negative.
- Depends on `tzdata`, so time zone names work on Windows and slim images.
- Sign-in checks take one Redis call instead of three. Revoking a token, signing out, disabling a user or changing a role still applies on the next request. A token's "last used" time is now saved at most once a minute.
- Queueing a job takes 3 Redis round trips instead of 8. `/metrics` takes 2 instead of 15. The job list reads only the rows it returns.
- The Redis connection pool waits for a free connection (up to `REDIS_POOL_TIMEOUT`) instead of failing at once, and allows 200 connections per process instead of 100.
- Live dashboards share one Redis subscription per API process and refresh at most twice a second, instead of re-reading everything on every job change.
- 429 and 503 responses include `Retry-After`, and the log says why each was sent (at most once every 10 seconds per reason).
- Both compose files raise the open-file limit for the web and worker containers.
- Workers running different apps no longer replace each other's task lists; a task drops out about 5 minutes after nothing advertises it.
- The worker's span is now named `run <task>` (was `execute_task`).
- `npm run dev` proxies the API through Vite, so the dashboard is same-origin in development too.

### Fixed
- Cancelling a job that had been recovered from a crashed worker and later retried silently did nothing: cancel looked in the wrong priority list.
- A job attempt that failed after the job had been handed to another worker (its worker looked dead) scheduled a second copy of the job or dead-lettered it. Stale attempts are now ignored, checked atomically.
- Replaying the same dead letter twice at once (two clicks, two admins, "Replay all") queued the job several times. Each dead letter can now only be taken once; the loser gets `409`.
- One unreadable dead-letter entry broke the whole dead-letters page.
- Job records recreated by a replay, a retry or a dead-lettering never expired.
- A delay of `1e300`, `Infinity` or `NaN` crashed the request (500); `NaN` in a job's arguments was accepted and then broke the job list for everyone. Both are now a `422`.
- When stopping, a worker could start one more job after waiting for a free slot.
- `pytaskq ping` printed the Redis password from `REDIS_URL`, and counted dead workers.
- Starting several API processes with `PYTASKQ_ADMIN_USER` logged a startup error.

### Security
- **Sign-in limit could be bypassed with parallel requests:** 40 guesses sent at once were all checked, with a limit of 10. Attempts are now counted before the password is checked.
- **The per-IP sign-in limit trusted a client-sent `X-Forwarded-For` header**, so it could be dodged, or aimed at someone else's IP. The IP now comes from the connection, or from your reverse proxy via `FORWARDED_ALLOW_IPS`.
- **With the default `ALLOWED_ORIGINS=*`, any website could read API responses using a signed-in visitor's cookie** (credentials were allowed for every origin). Cookies are now only accepted cross-origin from origins you list.
- Webhook URLs pointing at loopback written as IPv6 (`::ffff:127.0.0.1`), carrier-grade NAT (`100.64.0.0/10`) and other non-public ranges are refused.
- Two browsers finishing first-run setup at the same moment could both create an admin.
- Newer FastAPI versions record the full URL in their own traces, which would include an invite link's secret. PyTaskQ turns that off and records route patterns only.

### Removed
- The unused per-minute rate limit (`RATE_LIMIT_PER_MINUTE`, and `rate_limit_used` / `rate_limit_max` in `/metrics`). It was never enforced; enabling it would have capped the whole server at 100 requests a minute.
- The unused 5-second duplicate filter and its settings (`IDEMPOTENCY_PENDING_TTL`, `DEDUP_TTL`). Identical jobs without an `Idempotency-Key` are separate jobs, as before.

## 0.3.1 — 2026-11-18

### Added
- **Official Docker image** `ghcr.io/vrajpatelm/pytaskq` for amd64 and arm64, published automatically with each release (`0.3.1`, `0.3`, `latest`). It runs as a non-root user and has a built-in health check.
- **Production setup** in `deploy/`: Caddy for automatic HTTPS, a private password-protected Redis, and a `backup` service that saves a snapshot every 6 hours and keeps 7 days.
- `deploy/restore.sh` puts a backup back safely: a safety backup first, then a correct load with the append-only log rebuilt and a key-count check.
- `docs/DEPLOYMENT.md`: a step-by-step self-hosting guide.
- CI builds the Docker image on every pull request and runs it end to end (`scripts/smoke-test-image.sh`).

### Changed
- The `Dockerfile` installs the package itself instead of the dev/test requirements, and only copies what the image needs.
- In the dev `docker-compose.yml`, the worker's health check now tests its Redis connection.

## 0.3.0 — 2026-11-18

### Added
- **Accounts for self-hosting.** First-run setup with a one-time code from the server log (or `PYTASKQ_ADMIN_USER` / `PYTASKQ_ADMIN_PASSWORD`), sign-in, Viewer / Operator / Admin roles, single-use invite links, and personal API tokens (`ptq_…`).
- **Settings tab** for your account, password, API tokens and team.
- `pytaskq users list | create | reset-password | set-role | delete | setup-code` for recovery from the server.
- Jobs record `created_by` and `cancelled_by`.
- **CI** on every push: tests on Python 3.10, 3.12 and 3.13, bug-finding lint, a dashboard build check, and a clean-install package check.
- **PyPI packaging** (`pyproject.toml`) with the dashboard bundled, plus optional `[demo]`, `[tracing]` and `[dev]` extras. Pushing a `v*` tag publishes a release automatically.

### Changed
- Sign-in is required by default. Set `PYTASKQ_AUTH=off` for local development.
- `uvicorn[standard]` is a dependency, so the live dashboard works on clean installs.
- numpy and Pillow are now optional (`pip install "pytaskq[demo]"`).
- The README is reorganised, and outdated docs and scripts were removed.

### Fixed
- The demo `resize_image` task failed on every call because of a missing import.
- Joining by invite or setup now records "last sign-in".

## 0.2.0

### Added
- `async def` tasks, keyword arguments, `delay_sync()`, `apply_async()`, and per-task `max_retries`, `timeout`, `priority` and `name`.
- `queue.status()`, `queue.wait()` and `queue.cancel()`. Every job has a status from the moment it's queued.
- Job history (`GET /tasks`), cancellation, and task discovery (`GET /tasks/registered`).
- CLI: `pytaskq dashboard`, `pytaskq tasks`, `pytaskq ping`, and worker sizing flags.
- A rebuilt dashboard: lifecycle strip, Jobs, Run a task (forms generated from signatures), Workers, Dead letters.

### Fixed
- Retries of timed-out jobs had their results discarded (stale fencing token).
- Retries and replays lost their priority.
- Webhooks only worked for the built-in demo app.
- The dashboard listed tasks the Docker worker didn't have.
- Pending counters could go negative. Tracing logged errors when Jaeger wasn't running.
