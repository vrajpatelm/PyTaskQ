# Writing tasks

Everything you can do from your own code: register tasks, queue jobs, check on them, report progress, cancel them, and limit how fast or how many run. For where task code runs and how timeouts stop it, see [WORKERS.md](WORKERS.md). For queues and schedules, see [QUEUES.md](QUEUES.md).

## A task

```python
from pytaskq import PyTaskQ

app = PyTaskQ()

@app.task(type="io", max_retries=5, timeout=30)
def send_receipt(order_id: int, email: str):
    """Email the receipt for an order."""
    ...
```

The first line of the docstring and the parameters (with their types and defaults) are shown in the dashboard's *Run a task* form, and the API checks a job's arguments against them before queueing it.

### Options

| Option | Default | What it does |
|---|---|---|
| `type` | `"async"` for `async def`, else `"cpu"` | Where it runs: `"cpu"` in its own process, `"io"` in a thread, `"async"` on the worker's event loop. Pick `"io"` for code that mostly waits (HTTP, email, databases), `"cpu"` for code that computes. |
| `queue` | `"default"` | The named queue it lives on. Only workers serving that queue run it ([QUEUES.md](QUEUES.md)). |
| `priority` | `"default"` | `"high"`, `"default"` or `"low"` within its queue. See [weighted priorities](QUEUES.md#priorities). |
| `max_retries` | `3` (`MAX_RETRIES`) | Retries after the first failure, with a random backoff of 1–30 s, before the job becomes a dead letter. |
| `timeout` | `300` s (`TASK_TIMEOUT`) | How long one attempt may run, counted from when it starts. `0` = no limit. |
| `cron` | — | A 5-field cron schedule: the fleet queues it on time, once per tick. |
| `tz` | `CRON_TIMEZONE` (UTC) | The time zone the schedule is read in, e.g. `"Europe/London"`. Only for cron tasks. |
| `catch_up` | `CRON_CATCH_UP` (on) | Only for cron tasks: after an outage, run the latest missed tick once. [Details](QUEUES.md#missed-runs-and-catch-up). |
| `max_concurrency` | no limit | At most this many jobs of this task run at once, **across all workers**. |
| `rate_limit` | no limit | At most this many starts per second, minute or hour, **across all workers**: `"10/s"`, `"100/m"`, `"1000/h"`, `"5/10s"`. |
| `name` | the function's name | The name jobs use. Keep it stable: waiting jobs refer to it. |

App-wide defaults: `PyTaskQ(default_priority="low", default_queue="emails", max_retries=5, timeout=60)`.

A wrong option (an unknown type, a misspelt time zone, `rate_limit="fast"`, `max_concurrency=0`) is an error when the module is imported, not a surprise in production.

## Queueing jobs

```python
job = await send_receipt.delay(1001, email="ada@example.com")          # from async code
job = send_receipt.delay_sync(1001, email="ada@example.com")           # from plain code (no event loop running)
job = await send_receipt.schedule(1001, email="ada@example.com", delay_seconds=3600)   # in an hour
job = await send_receipt.apply_async(                                  # every option
    [1001], {"email": "ada@example.com"},
    priority="high", queue="vip", countdown=10,
    idempotency_key=f"receipt-{order_id}",
    on_success_url="https://example.com/hooks/receipt",
    task_id="receipt-1001",
)
job["task_id"]
```

- `.delay()` and `.delay_sync()` also accept `priority=`, `queue=`, `on_success_url=` and `idempotency_key=` as keyword arguments, unless your function has a parameter with that name.
- **`idempotency_key`** makes a retried call safe: the same key (within `IDEMPOTENCY_TTL`, a day) returns the original job with `"duplicate": True` instead of queueing a second one. Use it whenever the caller might send the same request twice (a retry after a timeout, a double click).
- **`task_id`** lets you choose the id. Reusing the id of a job that is still waiting or running raises `TaskIdInUse`; the id of a finished job can be reused.
- **`on_success_url`** gets a signed `POST` when the job succeeds (header `X-PyTaskQ-Signature: sha256=…`, HMAC of the body with `WEBHOOK_SECRET`). Delivery is itself a job, so it's retried.
- Jobs over `MAX_PAYLOAD_BYTES` (1 MB) raise `ValueError`. Pass a file's location instead of its contents.

From other languages or scripts, use `POST /task/enqueue` ([API.md](API.md)).

## Checking on a job

```python
info = await app.status(job["task_id"])       # None once the record has expired
info["status"]        # Queued, Scheduled, Processing, RetryScheduled, Success, DeadLetter or Cancelled
info["result"]        # strings as returned; anything else as JSON text

info = await app.wait(job["task_id"], timeout=30)   # until it finishes; TimeoutError otherwise
```

`wait()` checks every 20 ms at first and less often as time passes, so a quick job's result comes back at once without a long wait flooding Redis.

A finished job's record is kept for `RESULT_TTL` (a day), but only the newest `FINISHED_JOBS_KEPT` (50,000) finished jobs are kept; older ones go first. Search them in the dashboard's **History** tab or with `GET /tasks/history`.

## Reporting progress

```python
from pytaskq import progress

@app.task(type="io")
def import_rows(path: str):
    rows = read_csv(path)
    for i, row in enumerate(rows, 1):
        save(row)
        progress(100 * i / len(rows), f"{i} of {len(rows)} rows")
```

- `progress(percent, message)`: a percentage from 0 to 100 (values outside are clamped), a short message (up to 200 characters), or both.
- The dashboard shows a progress bar on running jobs; `GET /task/{id}` and `app.status()` return `progress` and `progress_message`.
- **Call it as often as you like.** The worker keeps only the newest value and writes it to Redis about once a second.
- It works in every task type; CPU tasks send it from their process.
- Outside a job (calling the function directly, in a unit test) it does nothing and returns `False`.
- A retry starts again from no progress.

## Cancelling a job

From the dashboard (the job's **Cancel job** / **Stop job** button), the API (`POST /task/{id}/cancel`) or your code:

```python
await app.cancel(job_id)      # True if it was cancelled or is being stopped
```

| The job is… | What happens |
|---|---|
| Waiting (queued, scheduled, waiting for a retry) | Removed at once. Status `Cancelled`. |
| Running | Its worker stops it within about a second, then its status is `Cancelled` and it isn't retried. |
| Finished | Nothing (`False`, or `409` from the API). |

How a running job is stopped depends on where it runs:

- **`async def`**: the coroutine is cancelled (`asyncio.CancelledError` is raised at its current `await`, so `try`/`finally` blocks run).
- **`type="cpu"`**: its process is stopped and a fresh one takes its place.
- **`type="io"`**: Python can't stop a thread. The job is recorded as cancelled at once and whatever it returns later is ignored, but its code keeps running until it returns. For long IO tasks, check `cancelled()` now and then:

```python
from pytaskq import cancelled

@app.task(type="io", timeout=3600)
def export_everything():
    for page in pages():
        if cancelled():
            return "stopped early"
        upload(page)
```

`current_task_id()` returns the id of the job the code is running for, handy for logging.

## Limiting how many run, and how fast

Some work must not run too much at once: an account that allows two simultaneous logins, an API with a quota, a database that can only take a few heavy queries.

```python
@app.task(type="io", max_concurrency=2)
def sync_crm_account(account_id): ...

@app.task(type="io", rate_limit="100/m")
def call_partner_api(item_id): ...
```

Both limits hold **across every worker**, not per worker, and both are kept in Redis.

**`rate_limit`** spreads starts evenly: `"100/m"` starts at most one job every 0.6 seconds, `"10/s"` one every 0.1 s. Each job reserves its start time when a worker first takes it, then waits in the schedule until then (once; it doesn't keep checking). A job waiting for its turn shows `Queued` with *Waiting for its turn under rate_limit=100/m: starts at 14:03:05 UTC* in the dashboard.

**`max_concurrency`** counts the jobs of that task running right now. A job over the limit goes back to the schedule and checks again after a short wait that grows up to 10 seconds. If a worker dies while running one, its place frees up after about a minute (2 × `WORKER_HEARTBEAT_TTL`): it's a lease the worker renews while the job runs.

Waiting for either limit **never uses up a retry**, and a retry takes a new turn. The limits apply to starts, not to how long a job runs.

Good to know:

- They're best for keeping a modest amount of work in line. With thousands of jobs waiting behind `max_concurrency=1`, each one checks back every few seconds; give such work its own queue and a worker with a small `WORKER_IO_WORKERS` instead.
- They're per task name. Two tasks that share a quota need to share a task (or a queue with a dedicated worker).
- Only workers running 0.5 or later enforce them.

## Pausing a queue

```python
await app.pause_queue("emails")     # workers start no new jobs from it
await app.resume_queue("emails")
```

Or the pause button next to each queue in the dashboard, or `POST /queues/emails/pause`. Pausing takes effect at once: running jobs finish, new jobs can still be queued and simply wait, and scheduled jobs keep moving onto the queue. Useful during an incident with a downstream service, or a deploy of the code a queue's tasks call.

## Writing tasks that behave well

- **Safe to run twice.** PyTaskQ runs every job at least once: after a crash, a timeout or a lost connection it may run again. Use an idempotency key in the systems you call, or check whether the work is already done. [GUARANTEES.md](GUARANTEES.md) explains exactly when this happens.
- **Give network calls their own timeout** in IO tasks (`requests.get(url, timeout=10)`).
- **Never block the event loop** in `async def` tasks: use `await`, or make it a plain function with `type="io"`.
- **Keep arguments and results small.** Store files and big data elsewhere and pass their location.
- **Report progress** for anything that takes more than a few seconds, and check `cancelled()` in long IO loops.
