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

A wrong option (an unknown type, a misspelt time zone, `rate_limit="fast"`, `max_concurrency=0`, a task name with spaces or line breaks) is an error when the module is imported, not a surprise in production. Task names are 1–200 letters, digits, `_`, `.`, `:` or `-`, starting with a letter or `_`.

## Queueing jobs

```python
job = await send_receipt.delay(1001, email="ada@example.com")          # from async code
job = send_receipt.delay_sync(1001, email="ada@example.com")           # from plain code (no event loop running)
job = await send_receipt.schedule(1001, email="ada@example.com", delay_seconds=3600)   # in an hour
                                                                         # (countdown= if the task has its own delay_seconds)
job = await send_receipt.apply_async(                                  # every option
    [1001], {"email": "ada@example.com"},
    priority="high", queue="vip", countdown=10,
    idempotency_key=f"receipt-{order_id}",
    on_success_url="https://example.com/hooks/receipt",
    task_id="receipt-1001",
)
job["task_id"]
```

- `.delay()` and `.delay_sync()` also accept `priority=`, `queue=`, `countdown=` (seconds to wait), `task_id=`, `on_success_url=` and `idempotency_key=` as keyword arguments, unless your function has a parameter with that name.
- **Arguments are checked when you queue the job**, against the function's signature: `send_receipt.delay(1, 2, 3, 4)` raises `TypeError` at once (the job isn't queued) instead of failing in a worker after every retry.
- **Arguments go through JSON.** Use `str`, `int`, `float`, `bool`, `None`, lists and dicts with text keys. Tuples arrive as lists. Anything that wouldn't arrive as you sent it raises `TypeError` naming the argument: a `datetime`, `set`, `bytes` or `Decimal` (send an ISO date string, a list, base64 text, a string), or a dict with non-text keys (`{1: "a"}` would arrive as `{"1": "a"}`, and `{1: "a", "1": "b"}` would lose a value).
- **`idempotency_key`** makes a retried call safe: the same key (within `IDEMPOTENCY_TTL`, a day) returns the original job with `"duplicate": True` instead of queueing a second one. Use it whenever the caller might send the same request twice (a retry after a timeout, a double click).
  A repeat after the first job's record is gone (`RESULT_TTL`) answers `"status": "unknown"`: the job ran long ago. `idempotency_key=""` is refused (`ValueError`), like an empty `Idempotency-Key` header.
- **`task_id`** lets you choose the id: text of 1–200 characters without line breaks or control characters (else `ValueError`). Reusing the id of a job that is still waiting or running raises `TaskIdInUse`; the id of a finished job can be reused.
- **`on_success_url`** gets a signed `POST` when the job succeeds (header `X-PyTaskQ-Signature: sha256=…`, HMAC of the body with `WEBHOOK_SECRET`). Delivery is itself a job, so it's retried. A URL that can never work (not http(s), no host, `localhost`, a private address written out) is refused when you queue the job.
- **`delay_sync()`** keeps one connection pool per process, so calling it from a web view costs one round trip, not a new connection each time.
- Jobs over `MAX_PAYLOAD_BYTES` (1 MB) raise `ValueError`. Pass a file's location instead of its contents.

From other languages or scripts, use `POST /task/enqueue` ([API.md](API.md)).

## Checking on a job

```python
info = await app.status(job["task_id"])       # None once the record has expired
info["status"]        # Queued, Scheduled, Processing, RetryScheduled, Success, DeadLetter or Cancelled
info["result"]        # strings as returned; anything else as JSON text

info = await app.wait(job["task_id"], timeout=30)   # until it finishes; TimeoutError otherwise
```

`wait()` checks every 20 ms at first and less often as time passes, so a quick job's result comes back at once without a long wait flooding Redis. For an id that has no record (a typo, or a finished job whose record is gone) it raises `LookupError` at once instead of waiting the whole timeout.

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
- Outside a job (calling the function directly, in a unit test) it does nothing and returns `False`, and so does a call after the job ended (from a thread or task it left running).
- The last call before the task returns is kept: a job that reports 99 % and returns shows 99 %.
- A retry starts again from no progress.
- **In threads the task starts itself** (`threading.Thread`, a `ThreadPoolExecutor`), wrap the function with `with_job()` so `progress()`, `cancelled()` and `current_task_id()` know the job:

```python
from pytaskq import with_job

with ThreadPoolExecutor(4) as pool:
    pool.map(with_job(download_one), urls)
```

## Cancelling a job

From the dashboard (the job's **Cancel job** / **Stop job** button), the API (`POST /task/{id}/cancel`) or your code:

```python
await app.cancel(job_id)      # "cancelled", "stopping" or False
```

| The job is… | What happens | `app.cancel()` returns |
|---|---|---|
| Waiting (queued, scheduled, waiting for a retry) | Removed at once. Status `Cancelled`. | `"cancelled"` |
| Running | Its worker stops it within about a second, then its status is `Cancelled` and it isn't retried, even if its code ran to the end meanwhile (its result is ignored). | `"stopping"` |
| Finished | Nothing (`409` from the API). | `False` |

Both strings are truthy, so `if await app.cancel(id):` works as before.

How a running job is stopped depends on where it runs:

- **`async def`**: the coroutine is cancelled (`asyncio.CancelledError` is raised at its current `await`, so `try`/`finally` blocks run). Let it propagate: a task that catches it and carries on (a bare `except:`, `except BaseException`, some retry helpers) is left running in the background after `CANCEL_GRACE` (5 s), with a warning; its slot is freed and its result ignored.
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

**`rate_limit`** spreads starts evenly: `"100/m"` starts at most one job every 0.6 seconds, `"10/s"` one every 0.1 s. Each job reserves its turn when a worker first takes it, then waits in the schedule until then (once; it doesn't keep checking). Right before it starts, the turn is checked again: two starts are never closer than the limit allows, even when turns passed while the queue was paused, the workers were busy or the jobs sat in the dead letters (those get a new turn instead of all starting at once). A cancelled job gives its turn back. A job waiting for its turn shows `Queued` with *Waiting for its turn under rate_limit=100/m: starts at 14:03:05 UTC* in the dashboard.

**`max_concurrency`** counts the attempts of that task running right now. A job over the limit waits in line; when a running one finishes, the job that has waited longest starts at once, with the free place kept for it. So the jobs run back to back, in the order they started waiting (the few a worker takes at the very same moment may swap places). Measured: 20 jobs of 0.1 s with `max_concurrency=1` took 2 seconds. Each place is a lease the worker renews while the job runs: if a worker dies, its places free up after about a minute (2 × `WORKER_HEARTBEAT_TTL`). An IO job that timed out or was cancelled keeps its place until its thread really returns (Python can't stop a thread), so the limit holds on bad days too.

Waiting for either limit **never uses up a retry**, and a retry takes a new turn. The limits apply to starts, not to how long a job runs.

Good to know:

- They're per task name. Two tasks that share a quota need to share a task (or a queue with a dedicated worker).
- Only workers running 0.5 or later enforce them (0.6 for the ordering and the stricter checks).

## Pausing a queue

```python
await app.pause_queue("emails")     # workers start no new jobs from it
await app.resume_queue("emails")
```

Or the pause button next to each queue in the dashboard, or `POST /queues/emails/pause`. Pausing takes effect at once: running jobs finish, new jobs can still be queued and simply wait, and scheduled jobs keep moving onto the queue. Scheduled (cron) runs of tasks on a paused queue are skipped, so they don't pile up and all run at once when you resume. Useful during an incident with a downstream service, or a deploy of the code a queue's tasks call.

## Failing for good: `Unrecoverable`

Some failures can't be fixed by trying again: bad input, a record that was deleted, a URL that's not allowed. Raise `Unrecoverable` and the job goes to dead letters at once, without using its retries:

```python
from pytaskq import Unrecoverable

@app.task(type="io", max_retries=5)
def charge(order_id):
    order = db.get(order_id)
    if order is None:
        raise Unrecoverable(f"order {order_id} doesn't exist")
    ...
```

The dead letter says *Failed, not retried (retrying can't help)*. Webhook deliveries to a URL that isn't allowed fail the same way.

## Several jobs as one: workflows

Run steps one after another or at the same time, passing results on, shown as one workflow in the dashboard:

```python
from pytaskq import chain, group

await app.workflow(chain(download.s(url), group(resize.s(size=64), resize.s(size=512)), publish.s()))
```

Everything about them: [WORKFLOWS.md](WORKFLOWS.md).

## Writing tasks that behave well

- **Safe to run twice.** PyTaskQ runs every job at least once: after a crash, a timeout or a lost connection it may run again. Use an idempotency key in the systems you call, or check whether the work is already done. [GUARANTEES.md](GUARANTEES.md) explains exactly when this happens.
- **Give network calls their own timeout** in IO tasks (`requests.get(url, timeout=10)`).
- **Never block the event loop** in `async def` tasks: use `await`, or make it a plain function with `type="io"`.
- **Run native code that may crash or hold the GIL as `type="cpu"`**: image, PDF and ML libraries, huge `sort`/`json`/`pickle` calls, regular expressions on user input. In an IO or async task such code can freeze or kill the whole worker (its heartbeat stops too, so the job is handed to another worker); after `MAX_RECOVERIES` (3) such takeovers the job is dead-lettered. In a CPU task it only costs its own process.
- **Don't wait for a job from inside a job** (`await app.wait(child)` in a task). The waiting job holds a worker slot while the other one needs one; when every slot holds a waiting job, nothing moves. Use a [workflow](WORKFLOWS.md) to run jobs one after another (the worker logs a warning when a task waits like this).
- **Keep arguments and results small.** Store files and big data elsewhere and pass their location.
- **Report progress** for anything that takes more than a few seconds, and check `cancelled()` in long IO loops.
- **Fail by raising an exception.** `sys.exit()`, `exit()` or `KeyboardInterrupt` inside a task (argparse and click call `sys.exit` on bad input) fail that job with `TaskExited`; they never stop the worker.
- **Return plain JSON values** (dict, list, str, int, float, bool, None). `NaN`/`Infinity` are stored as `null` and sets, bytes or objects as their text, with a warning in the worker's log (an object whose `__str__` raises is stored as `<unreadable Name>`; the job still succeeds). Text that isn't valid UTF-8 (file names from `os.listdir()` on Linux, for example) is stored with `\udcff`-style escapes.
- **Decorators are fine.** An `async def` behind an ordinary decorator still runs as an async task, a returned coroutine is run, and a generator's items are collected into a list (its code only runs when it's read).
- **Errors are cut** to `MAX_ERROR_BYTES` (4 KB: the start and the end) before they're stored, logged and sent to the dashboard. Put big details in your own logs.
