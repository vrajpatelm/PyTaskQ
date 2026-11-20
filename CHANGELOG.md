# Changelog

All notable changes to PyTaskQ. Versions follow [semantic versioning](https://semver.org/).

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
