# Performance tuning

PyTaskQ's defaults handle a few hundred requests a second on a small server. This guide explains what limits that number, how to tell which limit you're hitting, and which setting to change. None of the settings here weaken security or risk losing jobs.

**Start with the dashboard.** **Settings → Performance** shows what PyTaskQ had to turn away in the last 15 minutes, and suggests the setting to change, ready to copy into `.env`. It also warns when Redis is heading for full, when workers run different versions or settings, and when scheduled runs were missed. For graphs over time, scrape `/metrics/prometheus` ([DEPLOYMENT.md](DEPLOYMENT.md#monitoring)).

## What a 2-core server handles

Measured with the load test in [`loadtest/`](../loadtest) on 2 CPU cores, with sign-in on, the API, a worker and Redis on the same machine, and `WORKER_IO_WORKERS=32`. Each simulated user queues a job, checks it, and looks at the job list and metrics, with a pause of 0.5 to 2 seconds between actions. Every stage ran for 2 minutes.

| 500 users at once | 0.3.1 | 0.4 |
|---|---|---|
| Requests answered per second | 282 | 350 |
| Typical response (median) | 320 ms | 16 ms |
| Slowest 5% of responses | 730 ms | 130 ms |
| Refused: "Redis unavailable" (503) | 14.3% | **0%** |
| Refused: "queue full" (429) | 0% | 7.9% |
| Jobs finished per second | 121 | 146 |

The 429s in 0.4 are not an error. 500 users created about 175 jobs a second, more than one worker on 2 cores can finish (about 146 a second for this test job). The queue absorbed the difference until it was full, and then PyTaskQ told clients to retry instead of letting Redis fill up. In 0.3.1 the same overload showed up as 503 errors spread across *every* endpoint, including just looking at the dashboard.

At 400 users, who create jobs a little slower than the worker finishes them, 0.4 refused nothing: 291 requests a second, a median response of 8 ms, the slowest 5% under 60 ms, and only 13 jobs left waiting at the end. That's the sustained capacity of this setup. See [Sizing](#sizing) for what to add when you need more.

## What changed in 0.5

| | 0.4 | 0.5 |
|---|---|---|
| Time from queueing a job to an idle worker starting it | up to 1 s (about 475 ms on average) | 1–3 ms |
| Redis commands per second from one idle worker (1 queue / 20 queues) | 15 / 65 | 10 / 12 |
| Redis memory after a million jobs | grows with jobs per day × `RESULT_TTL` | flat: the newest `FINISHED_JOBS_KEPT` finished jobs |

- **Instant pickup.** Idle workers used to check their queues every so often. Now queueing a job also drops a small wake-up token, and an idle worker waits on those tokens, so it starts the job at once and asks Redis almost nothing while idle.
- **Bounded memory.** Finished jobs used to stay for `RESULT_TTL` (a day) each, so memory grew with traffic: 100 jobs a second filled about 20 GB a day. Now at most `FINISHED_JOBS_KEPT` (50,000) finished jobs are kept, the oldest going first. Measured: 10,000 jobs with a cap of 2,000 kept Redis at a flat 6 MB.
- **A forecast.** *Settings → Performance* measures how fast jobs finish and how big their records are, works out how much memory the kept records will settle at, and warns when that would take Redis past 90% of `maxmemory`, with roughly when ("Redis will be full in about 25 minutes at the current rate"), before anything is refused.

## What changed in 0.4

Every request used to make several Redis calls one after another, and each call held one of only 100 connections while it waited. Under load the connections ran out, and requests failed with 503 instead of waiting.

| Request | Redis round trips before | After |
|---|---|---|
| Checking who's signed in (every request) | 3, including a write | 1 (a write at most once a minute) |
| Queueing a job | 8 | 3 |
| `GET /metrics` | 15 | 2 |
| `GET /tasks?limit=50` | reads all 500 history entries | reads only the newest 50 |
| Live dashboard | 12 calls on every job change, for each open tab | 1 call, at most twice a second |

And the connection pool now **waits** up to 5 seconds for a free connection instead of failing at once.

Nothing is cached in a way that changes behaviour: revoking a token, ending a session, disabling a user or changing a role still takes effect on the very next request.

## 429 or 503: which one, and what to do

| You see | It means | Fix |
|---|---|---|
| **429** "Server is busy: queue '…' is full" on `POST /task/enqueue` | That queue holds `QUEUE_CAPACITY` jobs. Workers serving it are finishing jobs more slowly than they arrive. The limit is exact: with `QUEUE_CAPACITY=10`, 200 simultaneous requests get exactly 10 jobs in. | Add workers, or raise `WORKER_IO_WORKERS`. Raise `QUEUE_CAPACITY` only to absorb short bursts. |
| **503** "Redis is unavailable", detail "The server is busy" | Every Redis connection stayed busy for `REDIS_POOL_TIMEOUT` seconds. | Raise `REDIS_MAX_CONNECTIONS`. If the server's CPU is at 100%, it needs more cores instead. |
| **503** "Redis is unavailable", detail "temporarily down" or "not answering" | Redis couldn't be reached, or didn't answer in time. | `docker compose logs redis`. Check `REDIS_URL` / `REDIS_HOST`. |
| **503** "Redis is full" | Redis reached `maxmemory`: new jobs are refused until workers finish waiting ones. | Raise `maxmemory`, add workers, or keep less (`RESULT_TTL`, `HISTORY_LIMIT`, old dead letters). See [REDIS.md](REDIS.md). |

Both responses carry a `Retry-After` header. Well-behaved clients wait that many seconds and try again, so a short overload causes a short delay, not lost work.

The server log says which one happened, at most once every 10 seconds per kind:

```bash
docker compose logs web | grep "Turned a request away"
```

## Settings

All of these go in `.env` (or the environment). Restart after changing them.

| Setting | Default | What it does |
|---|---|---|
| `WORKER_IO_WORKERS` | cores × 4, at least 4 | Jobs each worker runs at once, besides CPU jobs. The biggest lever for jobs that mostly wait: HTTP calls, email, database queries, `async` tasks. Doesn't help CPU-heavy jobs. |
| `WORKER_CPU_WORKERS` | cores − 1 | Processes for `type="cpu"` jobs. More than the number of cores doesn't help. |
| `QUEUE_CAPACITY` | 500 | Jobs that can wait in **each** queue before new API jobs for it get 429 (`0` = no limit). Jobs your app queues with `.delay()` aren't refused. Due scheduled jobs and retries move onto a queue only while it's below this, so a big backlog drains as workers keep up instead of flooding it. A bigger queue absorbs longer bursts but doesn't make workers faster. |
| `MAX_QUEUES` | 100 | How many queue names may exist. Unused names are forgotten automatically. `0` = no limit. |
| `WORKER_QUEUES` | the worker's tasks' queues | Which queues a worker serves (`--queues`). See [QUEUES.md](QUEUES.md). |
| `MAX_RESULT_BYTES` | 1,000,000 | Larger results aren't stored in Redis (the job still succeeds). `0` = no limit. |
| `MAX_PAYLOAD_BYTES` | 1,000,000 | Larger jobs (their arguments) are refused: `413` from the API, `ValueError` from `.delay()`. Each waiting job is held in Redis memory. `0` = no limit. |
| `DLQ_LIMIT` | 10,000 | Dead letters kept; the oldest are dropped beyond it. `0` = no limit. |
| `WEB_WORKERS` | 1 | API processes. `0` means one per CPU core. Same as `pytaskq dashboard --workers N`. |
| `REDIS_MAX_CONNECTIONS` | 200 | Redis connections per process. Redis accepts 10,000 by default, so raising this is safe. A value in `REDIS_URL` (`?max_connections=1000`) also works. |
| `REDIS_POOL_TIMEOUT` | 5 | Seconds a request waits for a free connection before a 503. |
| `LIVE_UPDATES_PER_SECOND` | 2 | How often an open dashboard refreshes at most. |
| `HISTORY_LIMIT` | 500 | How many recent jobs the Jobs list can show. The **History** tab searches every kept finished job. |
| `FINISHED_JOBS_KEPT` | 50,000 | Finished jobs whose records are kept; the oldest go first. Each takes about 1–3 KB (more with big arguments or results), so 50,000 is roughly 50–150 MB. `0` = no cap (then only `RESULT_TTL` limits them). |
| `RESULT_TTL` | 86,400 | Seconds a finished job's record is kept at most. |
| `PRIORITY_WEIGHTS` | `6,3,1` | How a worker shares its turns between high, default and low jobs; `strict` = high always first. See [QUEUES.md](QUEUES.md#priorities). |

In Docker, each container can open only 1024 files by default, and every client connection and Redis connection uses one. The compose files in this repo raise that to 65,536 (`ulimits: nofile`). Do the same if you write your own.

## Sizing

- **Give heavy work its own queue.** Put slow or bursty tasks on their own queue (`@queue.task(queue="images")`) and run a worker for it with `--queues images`, on a bigger machine if needed. A flood of image jobs then can't delay emails, and each queue has its own `QUEUE_CAPACITY`. See [QUEUES.md](QUEUES.md).
- **Workers do the work.** If jobs pile up (429s), add workers before anything else. Workers can run on any machine that reaches your Redis, and they share the queue automatically:
  ```bash
  docker compose up -d --scale worker=3          # more on the same machine
  pytaskq worker --app tasks:queue               # or on another machine, with REDIS_URL set
  ```
- **2 cores:** keep `WEB_WORKERS=1`. The API and the worker share the cores, and a second API process would take CPU from the worker. Raise `WORKER_IO_WORKERS` if your jobs mostly wait.
- **4 or more cores:** set `WEB_WORKERS` to about half the cores, and leave the rest for workers. On a machine that only runs the API, use one per core (`WEB_WORKERS=0`).
- **Redis** is rarely the limit. In these tests it needed less than half a core.

## Load-test your own server

Run the test from **another computer**, so the test itself doesn't use the server's CPU.

```bash
pip install locust
```

Create an API token (**Settings → API tokens**, operator or admin), then put two lines in a `.env` file in the folder you run the test from:

```bash
LOADTEST_HOST=https://jobs.example.com
PYTASKQ_TOKEN=ptq_...
```

Run each stage for a few minutes and save the report:

```bash
locust -f loadtest/locustfile.py --headless -u 100 -r 10 -t 3m --html report-100.html
locust -f loadtest/locustfile.py --headless -u 500 -r 20 -t 3m --html report-500.html
```

At the end it prints how many jobs the workers finished during the test, and how many are still waiting:

```
── Job processing during the test ──────────────────────
  jobs finished by workers : 17487  (145.7 per second)
  still waiting in queue   : 12
```

If "still waiting" stays small, the server keeps up at that load. If it grows towards `QUEUE_CAPACITY`, workers are the limit. Then check **Settings → Performance** for what to change.

The test queues the example app's `notify_user` task. To test one of your own tasks, set `PYTASKQ_TASK=your_task` and `PYTASKQ_KWARGS='{"order_id": 1}'`.
