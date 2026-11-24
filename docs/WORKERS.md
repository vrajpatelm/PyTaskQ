# How workers run your tasks

This guide explains where your task code runs, what a `timeout` really does for each kind of task, and what a worker does when something goes wrong: a crashed process, a stuck call, Redis going away for a moment, or a shutdown in the middle of a job. It ends with a short checklist for writing tasks that behave well.

## How a worker takes jobs

An idle worker waits for a wake-up token that queueing a job leaves on its queue, so it starts a new job within a few milliseconds and puts almost no load on Redis while idle. (It also looks every 5 seconds anyway, for jobs queued by older versions.) It takes jobs only from the queues it serves and that aren't paused, sharing its turns between priorities by `PRIORITY_WEIGHTS` ([QUEUES.md](QUEUES.md#priorities)).

## Where each task runs

| Task | Runs in | How many at once (per worker) |
|---|---|---|
| `type="cpu"` (the default for a plain `def`) | Its own **process**, one job per process | `WORKER_CPU_WORKERS` (cores − 1) |
| `type="io"` | A **thread** | `WORKER_IO_WORKERS` (cores × 4) |
| `async def` | The worker's event loop | Up to the worker's total job limit |

A worker takes at most *CPU processes + IO threads* jobs from Redis at a time. A job taken while every process (or thread) of its kind is busy waits inside the worker until one is free.

The CPU processes start with the worker, and each one imports your task modules first, so the first jobs don't pay for that start-up.

## Timeouts

A job's `timeout` counts from the moment **its function starts**, not from when the worker took it from the queue. Waiting for a free process or thread doesn't count. (Before 0.4.1 it did, and a burst of CPU jobs could time out without ever running.)

What happens when the time is up depends on where the task runs:

| Task | At its timeout |
|---|---|
| `type="cpu"` | Its process is **stopped** and a fresh one takes its place. Other CPU jobs keep running. |
| `async def` | The coroutine is **cancelled**. |
| `type="io"` | Python can't stop a thread, so the code **keeps running** until it returns. The job is still marked as timed out and retried, and whatever the old attempt returns later is ignored. |

Every timeout counts as a failed attempt: the job is retried (with backoff) up to `max_retries`, then dead-lettered.

### IO tasks: give network calls their own timeout

Because a thread can't be stopped, a timed-out IO task keeps its thread busy until its code returns. The worker doesn't hand that thread new work meanwhile, so the next IO jobs wait for a free thread and their own timeouts stay fair.

If **every** IO thread is stuck like this, the worker stops taking IO jobs and leaves them in the queue for other workers, while it keeps running CPU and async jobs. The Workers tab shows "N IO threads stuck", and *Settings → Performance* explains it.

The fix is in the task: set a timeout on the call itself.

```python
@queue.task(type="io", timeout=30)
def fetch(url):
    return requests.get(url, timeout=10).text        # not requests.get(url)
```

### Async tasks: never block the event loop

An `async def` task shares the event loop with everything else in the worker. A blocking call inside it (`time.sleep`, `requests.get`, a sync database driver) freezes the whole worker until it returns, and the timeout can't fire either.

```python
@queue.task
async def bad():
    time.sleep(30)                  # blocks every other job on this worker

@queue.task
async def good():
    await asyncio.sleep(30)         # lets other jobs run

@queue.task(type="io")
def also_good():
    time.sleep(30)                  # plain function in a thread
```

The worker watches for this. When the loop is blocked for more than 5 seconds, its log names the async task responsible, the Workers tab shows "Blocked by an async task", and *Settings → Performance* says how to fix it. If the task finishes anyway, it counts as succeeded (its work is done), and the log says it overran its timeout.

A blocked worker still counts as alive: its heartbeat runs in a separate thread. Before 0.4.1 a blocked worker looked dead after 30 seconds, and the same job was started a second time on another worker.

## When a CPU process dies

If the process running a job dies (the Linux out-of-memory killer, a crash in a C extension, `os._exit()`), that job fails with an error like:

```
ProcessCrashed: The process running this task was killed by SIGKILL (often the out-of-memory killer).
```

It's retried like any other failure, and a fresh process takes the empty slot at once. Other jobs aren't affected. Before 0.4.1 one dead process froze the whole worker.

If the worker can't start new processes at all, it stops with exit code 1, so Docker or systemd restarts it, and puts its jobs back on their queues first.

## What task code can't do

A task can't stop or wedge the worker, whatever its code does:

| The task… | What happens |
|---|---|
| calls `sys.exit()` / `exit()`, raises `KeyboardInterrupt` (any task type) | The job fails with `TaskExited` (and is retried or dead-lettered as usual); the worker carries on. Only the worker's own Ctrl+C / SIGTERM stops it. |
| is an `async def` behind a plain decorator, or returns a coroutine | It runs as an async task; the coroutine is awaited. |
| returns a generator | Its items are collected into a list (its code only runs when it's read). |
| returns text with invalid UTF-8 (a file name from `os.listdir()`) or raises with it | Stored with `\udcff`-style escapes. |
| raises with a huge message | The error is cut to `MAX_ERROR_BYTES` (4 KB, start and end kept) before it's stored or logged. |
| returns `NaN`, a set, bytes, an object | Stored as `null` / its text, with a warning in the log. |

And whatever else goes wrong while saving a job's outcome (other than Redis being away), the job becomes a dead letter that says why, instead of staying in *Processing* and running again on every restart. *(Before 0.6, a task calling `sys.exit()` stopped the worker, and then every worker the job was handed to next.)*

## When Redis goes away for a moment

A job that finishes while Redis is unreachable keeps its result in memory, and the worker retries saving it until Redis is back. The log says so once:

```
[Redis] Couldn't save a result for task 3f2a… (ConnectionError: …). Retrying until Redis is back; nothing is lost.
```

Saving the outcome and removing the job from the worker's processing list happen in one Redis step, so a job can't end up marked finished while still listed as running, or listed as running forever.

## When Redis is full

With `maxmemory` reached, Redis refuses new jobs, but workers keep taking, running and finishing the waiting ones, since that's what frees memory. A worker started while Redis is full starts anyway and says so:

```
[Startup] Redis is out of memory (402 MB used, maxmemory 400 MB). Starting anyway: taking and finishing waiting jobs frees memory.
```

A wrong Redis password stops the worker at startup with exit code 1, since retrying can't fix it. [REDIS.md](REDIS.md) covers the Redis settings that matter.

## Stopping a worker

Ctrl+C, `docker stop` or SIGTERM:

1. The worker stops taking new jobs.
2. Running jobs get up to `WORKER_SHUTDOWN_GRACE` seconds (default 30) to finish.
3. Jobs still running after that are stopped (CPU processes are killed, async tasks cancelled) and **put back at the front of their queue**, so another worker runs them. An IO thread can't be stopped, so the worker exits without waiting for it.
4. The worker leaves the fleet, and its leftover processes are stopped.

A job stopped this way runs again from the start, so keep tasks safe to repeat (see the checklist below).

Docker's own limit is 10 seconds by default, after which it kills the worker outright. The compose files in this repo give workers `stop_grace_period: 40s`, so the 30-second grace period can finish. If you write your own, do the same.

## When a worker crashes

Each worker sends a heartbeat every `WORKER_HEARTBEAT_TTL / 3` seconds (10 by default). One worker, the leader, checks every 10 seconds for workers whose heartbeat is older than `WORKER_HEARTBEAT_TTL` (30 s) and puts their jobs back at the front of their queues. A job is therefore picked up again within about 40 seconds of its worker crashing.

**Faster recovery with a worker name.** Give each worker a stable name and a restarted worker puts its own unfinished jobs back the moment it starts:

```bash
WORKER_NAME=images-1 pytaskq worker --app tasks:queue
```

In Docker, `WORKER_NAME={hostname}` uses the container's name, which survives restarts. The compose files in this repo do this. Two live workers can't share a name: the second one waits for the first one's heartbeat to stop, and refuses to start if it doesn't. A restart is recognised by a boot id the worker leaves in its temp folder (`/tmp/pytaskq-worker-<name>.boot`), not by hostname and process id: two containers with the same `hostname:` (or `network_mode: host`) both run as process 1, and before 0.6 the second one took over the first one's running job.

Heartbeat times come from Redis's clock, not each machine's, so a server whose clock is off doesn't make its workers look dead.

## Cancelling a running job

Cancelling a running job (dashboard, `POST /task/{id}/cancel`, `app.cancel()`) puts a request in Redis. Every worker checks for requests about its own running jobs once a second (one Redis command, only while it's running something) and stops the job the way it would at a timeout:

| Task | When cancelled |
|---|---|
| `type="cpu"` | Its process is stopped and a fresh one takes its place. |
| `async def` | The coroutine is cancelled. |
| `type="io"` | The thread keeps running (Python can't stop it); `pytaskq.cancelled()` returns `True` from now on, and its result is ignored. |

The job is recorded as `Cancelled`, isn't retried, and its record says who cancelled it. A job that a worker had taken but not yet started is cancelled without running. [TASKS.md](TASKS.md#cancelling-a-job) shows `cancelled()`.

## Jobs that wait for their turn

A task with `max_concurrency` or `rate_limit` ([TASKS.md](TASKS.md#limiting-how-many-run-and-how-fast)) can be taken by a worker before it may start. The worker doesn't hold on to it: it puts the job back in the schedule until its turn (for `max_concurrency`, in line: a finishing job starts the one that waited longest), with the reason shown in the dashboard, and takes other work meanwhile. Waiting never uses up a retry. Jobs on a paused queue simply aren't taken. A job that was cancelled while it waited never starts: the cancel request is checked right before every start.

## Progress

`pytaskq.progress(percent, message)` inside a task stores the job's progress; the worker writes the newest value to Redis about once a second, and the dashboard shows it as a bar. CPU tasks send it from their process to the worker. [TASKS.md](TASKS.md#reporting-progress)

## Versions and settings across workers

Each worker's heartbeat includes its PyTaskQ version and the settings that must match everywhere (`RESULT_TTL`, `FINISHED_JOBS_KEPT`, `PRIORITY_WEIGHTS`, `TASK_TIMEOUT`, `MAX_RETRIES`, `WEBHOOK_SECRET` as a short fingerprint, and others). The dashboard compares them with its own, logs what differs, and lists it in *Settings → Performance*; the Workers tab shows each worker's version. Old and new versions side by side are expected for a few minutes during an upgrade, not for longer.

## Jobs that aren't valid

Something that isn't a valid job can end up in a queue: a hand-written `LPUSH`, a payload from an incompatible tool, a field such as `"priority": "urgent"`. Before 0.4.1 such jobs vanished silently. Now each becomes a **dead letter** named "(invalid job)", with the reason and the text that was sent, so you can see where it came from. It can't be replayed, only deleted.

## Limits

| Setting | Default | What it limits |
|---|---|---|
| `MAX_PAYLOAD_BYTES` | 1,000,000 | Size of a job (its arguments). The API answers `413`, `.delay()` raises `ValueError`. `0` = no limit. |
| `MAX_RESULT_BYTES` | 1,000,000 | Size of a stored result. Larger results aren't stored; the job still succeeds. |
| `DLQ_LIMIT` | 10,000 | Dead letters kept. Beyond it the oldest are dropped, and the worker logs a warning. `0` = no limit. |
| `QUEUE_CAPACITY` | 500 | Jobs waiting in each queue before the **API** answers `429`. Your app's `.delay()`, cron, retries and replays are never refused. `0` = no limit. |

Put large data in a file or object storage and pass its location to the task.

A job's record (status, arguments, result) never expires while the job is waiting or running, however long that takes. It's kept for `RESULT_TTL` (a day) once the job finishes, fails for good or is cancelled.

## Checklist for well-behaved tasks

- **Safe to run twice.** A job can run again after a crash, a timeout or a shutdown. Use an idempotency key in the systems you call, or check whether the work is already done. [GUARANTEES.md](GUARANTEES.md) lists exactly when.
- **IO tasks set timeouts on their own network calls.**
- **Async tasks never call blocking code.** Use `await`, or make the task a plain function with `type="io"`.
- **Large inputs and outputs go in storage**, not in the job.
- **CPU tasks that need a lot of memory get a machine with room**, or their own queue and worker (`@queue.task(queue="heavy")`, `--queues heavy`).
- **Long tasks report progress** with `progress()`, and long IO tasks check `cancelled()`.

## Settings

| Setting | Default | |
|---|---|---|
| `WORKER_CPU_WORKERS` | cores − 1 | CPU processes per worker |
| `WORKER_IO_WORKERS` | cores × 4 | IO threads per worker |
| `WORKER_SHUTDOWN_GRACE` | 30 | Seconds running jobs get to finish when the worker stops |
| `WORKER_HEARTBEAT_TTL` | 30 | Seconds without a heartbeat before a worker counts as dead |
| `WORKER_NAME` | random | Stable name (`{hostname}` allowed). Same as `pytaskq worker --name` |
| `TASK_TIMEOUT` | 300 | Default `timeout` for tasks that don't set one (`0` = none) |
| `MAX_ERROR_BYTES` | 4096 | A failed job's error text is cut to this |
