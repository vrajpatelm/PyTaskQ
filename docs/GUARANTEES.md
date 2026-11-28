# What PyTaskQ promises, and what it doesn't

Every job queue makes trade-offs. This page states PyTaskQ's plainly, so you can write tasks that are correct under them and run Redis the way they need. Each point says what you should do about it.

## Every job runs at least once

A job is never lost once `.delay()` (or `POST /task/enqueue`) has returned, as long as Redis keeps its data (see [Durability](#durability) below). Workers move a job into their own processing list in the same step that takes it off the queue, so a crash at any moment leaves it somewhere a recovery will find it.

But a job **can run more than once**:

| When | Why |
|---|---|
| A worker crashes or loses power mid-job | Its jobs are put back on their queue and run again, from the start (at most `MAX_RECOVERIES` times, below). |
| An attempt times out | It's retried. An IO task's thread can't be stopped, so its code may still finish; the retry waits until that thread returns, so the two never run at the same time. |
| A worker is stopped and its grace period runs out | Unfinished jobs go back on the queue. |
| A worker can't reach Redis for longer than `WORKER_HEARTBEAT_TTL` (30 s), or is frozen that long (a VM pause, a task holding Python's GIL) | The others think it's dead and run its jobs elsewhere, while it may still be running them. |
| Redis loses the last second of writes (a crash with `appendfsync everysec`) | A job's "finished" record can be lost, so it runs again. |

What PyTaskQ does guarantee is that **only one attempt's outcome counts**. Each job carries a fence token; a stale attempt's result or failure is ignored, so a job never ends up both succeeded and dead-lettered, or retried twice. A stale attempt that hasn't started yet (its worker lost Redis while waiting for a free slot, and the job went to another worker) doesn't start at all.

A **lost reply** doesn't make duplicates either. If the connection drops after Redis has done something but before the answer arrives (a network blip, a failover), the client sends the command again; PyTaskQ recognises the repeat: a job is queued once (the caller gets "queued", not an error), a failure is one dead letter, a success counts once. A job a worker took but never received goes back on its queue within about a minute.

**Task code can't stop the worker.** `sys.exit()`, `KeyboardInterrupt`, a coroutine or generator returned by a decorator, text that isn't valid UTF-8, a 20 MB error message, a result whose `__str__` raises: each becomes a normal outcome for that one job (failed, or stored cleanly). An `async def` task that catches `CancelledError` and carries on can't hold the worker either: after `CANCEL_GRACE` (5 s) its slot is freed and whatever it does is ignored.

**One job can't take the fleet down.** Code that kills its whole process (a segfault in a C extension, the out-of-memory killer, `os._exit()`) or holds Python's GIL past the heartbeat takes its worker with it. The job is handed to another worker, which may die the same way, so PyTaskQ counts these recoveries: after `MAX_RECOVERIES` (3) the job goes to dead letters with the reason, and the fleet carries on. In a `type="cpu"` task such a crash only fails that job.

**Temporary refusals don't lose or repeat jobs.** When Redis refuses writes for a while (read-only during a failover, `NOREPLICAS`, `MISCONF`, `BUSY` from another client's long script, `LOADING` after a restart), a worker keeps the finished job's outcome and saves it as soon as Redis takes writes again. The job isn't run a second time.

**What to do:** make tasks safe to repeat. Send an idempotency key to the services you call (most payment and email APIs accept one), use `INSERT … ON CONFLICT DO NOTHING`, or check whether the work is already done before doing it.

## Order

Jobs are **not** processed in a strict order. Within one queue and priority, older jobs are taken first, but several workers run jobs in parallel, retries come back later, and priorities are weighted (below). Don't rely on job B starting after job A finished; if B depends on A, queue B from inside A.

## Priorities share, they don't starve

By default (`PRIORITY_WEIGHTS=6,3,1`), out of every 10 jobs a worker takes while all three priorities have work waiting, about 6 are `high`, 3 `default` and 1 `low`. When a priority has nothing waiting, its turns go to the others, so with only one kind of work every turn takes it. `PRIORITY_WEIGHTS=strict` restores "high always first", which lets a steady stream of high jobs keep low ones waiting forever. [QUEUES.md](QUEUES.md#priorities).

## Timeouts

A `timeout` counts from when the task's function starts. At the timeout an `async def` task is cancelled, a CPU task's process is stopped, and an IO task's thread is abandoned (Python can't stop a thread: its code keeps running until it returns, and its result is ignored). [WORKERS.md](WORKERS.md#timeouts).

**What to do:** give network calls inside IO tasks their own timeouts.

## Durability

Jobs live in Redis, so PyTaskQ is exactly as durable as your Redis:

| Redis setup | If the Redis machine fails |
|---|---|
| No persistence | Everything since the start is lost. Only for development. |
| `appendonly yes`, `appendfsync everysec` (the shipped configs) | Up to the last second of writes is lost. |
| `appendfsync always` | Nothing written is lost, but every write waits for the disk. |
| A replica with Sentinel, plus `REDIS_WAIT_REPLICAS=1` | The machine can fail and a replica takes over within seconds. A job whose enqueue returned `"replicas": 1` is on two machines. |

And whatever the setup:

- **Redis must use `maxmemory-policy noeviction`.** Any other policy deletes jobs silently when memory runs out. PyTaskQ warns loudly at startup when it isn't set, and the leader checks again every minute: switching eviction on while workers run, or Redis evicting keys, is logged as an error. [REDIS.md](REDIS.md).
- **Keys of the wrong type** (another app using PyTaskQ's key names) never lose a job: the step is refused before anything changes, the job is kept, and the log names the key. Give PyTaskQ its own keys with `REDIS_KEY_PREFIX` when Redis is shared.
- An `Idempotency-Key` is stored in the same Redis, so it can't protect against losing that same second.

## What's kept, and for how long

| What | Kept |
|---|---|
| A waiting or running job's record | As long as it waits or runs |
| A finished job's record (status, arguments, result, error) | `RESULT_TTL` (a day), and only the newest `FINISHED_JOBS_KEPT` (50,000) finished jobs. The oldest go first. |
| Dead letters | The newest `DLQ_LIMIT` (10,000) |
| The Jobs list | The newest `HISTORY_LIMIT` (500) jobs; the **History** tab searches every kept finished job |
| Idempotency keys | `IDEMPOTENCY_TTL` (a day) |

Because finished jobs are capped by count, Redis memory stays flat however many jobs a day you run. *Settings → Performance* forecasts when Redis would fill up at the current rate.

## Clocks

All times that machines compare (heartbeats, when a scheduled job is due, cron ticks) and every job's `created_at` use Redis's clock, not each machine's. A server whose clock is wrong doesn't make its workers look dead, run schedules at the wrong time, or show jobs created in the past.

## Scheduled (cron) tasks

- Each tick runs **once across the whole fleet**, however many workers run the schedule, and once when clocks go back an hour.
- A tick is normally queued within about 10 seconds of its time. If no worker was running at the time (an outage, a long deploy), the ticks are **missed**: PyTaskQ logs and counts them (*Settings → Performance*, Prometheus), and with `catch_up` (the default) it runs the latest missed tick once, late, when a worker is back. It never runs every missed tick: a task that runs every minute doesn't run 600 times after a 10-hour outage.
- A run that takes longer than the interval **overlaps** the next one (a warning says so). With `overlap=False` on the task, a tick is skipped while the previous run is still waiting or running.
- Leadership moves within about 2 seconds when the leader stops cleanly (it hands the lock over), and within about 12 seconds when it crashes. Until then nothing promotes scheduled jobs or retries.
- Keep at least one worker running at all times, ideally two, so one can take over.

## Limits across the fleet

`max_concurrency` and `rate_limit` hold across every worker, because they're kept in Redis.

- A `max_concurrency` place is a lease per attempt that the worker renews while the job runs, and an IO job that timed out keeps it until its thread really returns. If a worker is cut off from Redis or frozen for longer than a lease lasts (2 × `WORKER_HEARTBEAT_TTL`, a minute by default), its leases run out and other jobs may take those places, so for a short time the jobs that frozen worker was running come on top of the limit.
- `rate_limit` starts are never closer than the limit allows: a turn that passed while the job couldn't start (a pause, a busy worker, a replay) is replaced by a new one. Rates above one start a second hold too (`"10/s"` gives about 10 starts a second). Starts are decided on Redis's clock, so workers whose clocks differ can't start jobs closer together. A job that had to wait for a free thread or process takes its turn again when it gets one, so a burst of jobs that queued up behind busy threads still starts a gap apart. On a loaded machine the first line of a task's code can run a few tens of milliseconds after its start, so two tasks' code may begin slightly closer than the limit; their starts never are. (Measured in a 20-minute chaos test at `"20/s"`: 37 of 4,504 tasks' first lines came under 45 ms apart, all but 4 of them right after a Redis restart, a refused write or a killed worker; the closest was 22 ms.)

## Cancelling and pausing

- Cancelling a **waiting** job is immediate and certain: it won't run.
- Cancelling a **running** job stops it within about a second, and it ends **Cancelled** even if its code ran to the end meanwhile (an IO thread, an async task that ignored the cancel): whatever it returned is ignored and it isn't retried. Only a job that finished before its worker saw the request ends Success. `app.cancel()` says which case it was: `"cancelled"` (it was waiting), `"stopping"` (it was running), or `False`.
- A job that was cancelled while a worker had it (or whose worker died) **never starts again**: the request is checked right before every start, and kept (up to a week) until the job is cancelled. A request that comes too late to stop a run ends with that run: replaying the job, or queueing a new job with the same id, runs normally.
- **Pausing a queue** takes effect at once for new jobs; running jobs finish. Cron runs of its tasks are skipped while it's paused.

## Workflows

The next stage of a workflow starts exactly once, even when the last steps of a stage finish at the same moment or a worker dies in between. A step that runs twice is counted once, and a counted step is never re-run by the recovery sweep. Once a workflow is cancelled, none of its remaining steps starts. Each step runs on its task's own queue and priority. [WORKFLOWS.md](WORKFLOWS.md#what-it-guarantees)

## Connections

Every connection to Redis has a read timeout (`REDIS_SOCKET_TIMEOUT`, 15 s) and TCP keepalive, so a connection that a NAT, firewall or load balancer dropped without a word fails and is reopened instead of hanging. If a worker's own connections still stop answering while Redis is reachable, it reopens them after a minute and, if that doesn't help, stops (exit code 1) so its jobs go to other workers.

## Worker names

With a fixed `WORKER_NAME`, a worker that starts while another one with the same name is alive refuses to start, also when both are containers with the same hostname and process id 1. A restart of the same container or machine is recognised (it left a boot id in its temp folder) and puts its own jobs back at once.

If a worker is frozen longer than its heartbeat (a VM or laptop pause) and a new worker takes its name meanwhile, the old one notices at its next heartbeat and stops (exit code 1) without touching the jobs listed under that name.

## Running different versions or settings

During an upgrade, old and new workers run side by side for a few minutes. That's supported between neighbouring versions. Several settings must be the same in every process (`RESULT_TTL`, `FINISHED_JOBS_KEPT`, `PRIORITY_WEIGHTS`, `WEBHOOK_SECRET` and others): the dashboard compares every worker's version and settings with its own and says what differs in *Settings → Performance*. `REDIS_KEY_PREFIX` must match too, or they don't see each other at all.

## Limits on size

| Setting | Default | |
|---|---|---|
| `MAX_PAYLOAD_BYTES` | 1 MB | A job's arguments; larger jobs are refused (`413`, `ValueError`) |
| `MAX_RESULT_BYTES` | 1 MB | A stored result; larger ones aren't stored, the job still succeeds |
| `MAX_ERROR_BYTES` | 4 KB | A failed job's error text (the start and the end are kept) |
| `QUEUE_CAPACITY` | 500 per queue | Jobs waiting before the **API** answers `429`; your app's `.delay()` is never refused. Scheduled jobs and retries move onto a full queue as it empties; one full queue never holds up another's |
| `MAX_QUEUES` | 100 | Queue names |

## In one sentence each

- Write tasks that are safe to run twice.
- Use `noeviction`, `maxmemory` and `appendonly yes` on Redis.
- Give IO tasks' network calls timeouts, never block an `async def` task, and re-raise `CancelledError`.
- Run native code that may crash or hold the GIL as `type="cpu"`.
- Don't wait for a job from inside a job: use a workflow.
- Keep arguments and results small.
- Keep a worker (better: two) running at all times for schedules.
- Use the same `.env` for the dashboard and every worker.
