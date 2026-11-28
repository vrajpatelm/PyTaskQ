# Queues, workers and schedules

This guide covers how jobs are routed to workers, how to give a kind of work its own workers, how priorities share a worker's time, pausing a queue, how scheduled (cron) tasks pick their time zone and what happens to runs that were missed, and large results.

## Named queues

Every task lives on a **queue**, `default` unless you say otherwise. Every worker **serves** some queues and only takes jobs from those. Within a queue, jobs still have a priority (`high`, `default`, `low`).

Use queues to give a kind of work its own workers:

- heavy image or video jobs on a big machine, emails on a small one;
- a slow, bursty import that shouldn't delay password-reset emails;
- two separate apps sharing one Redis.

### Putting a task on a queue

```python
from pytaskq import PyTaskQ

queue = PyTaskQ()

@queue.task(queue="images", type="cpu")
def make_thumbnail(path: str): ...

@queue.task                      # stays on "default"
def send_receipt(order_id: int): ...
```

To put every task of an app on one queue, set it once: `PyTaskQ(default_queue="images")`.

A single job can go elsewhere: `await make_thumbnail.delay("a.png", queue="bulk")`, or `"queue": "bulk"` in the body of `POST /task/enqueue`.

Queue names are 1 to 50 lowercase letters, digits, `-` or `_`.

A queue exists once a task lives on it or a worker serves it. Through the API, `"queue"` must name an existing queue: `"queue": "imgaes"` gets `422 Unknown queue 'imgaes'` with the list of known queues, instead of a job no worker would ever take. Your app's code (`.delay(queue=...)`) can create queues freely.

At most `MAX_QUEUES` (100) queue names can exist. Names that nothing uses any more (no waiting jobs, no worker serving them, no task on them, no scheduled job for them) are forgotten after a few minutes, so one-off names don't pile up and slow down every dashboard refresh.

### Which queues a worker serves

By default, a worker serves **the queues its own tasks live on**. A worker started with the app above serves `default` and `images`, so it never takes a job it can't run. To choose yourself:

```bash
pytaskq worker --app tasks:queue --queues images            # only images
pytaskq worker --app tasks:queue --queues images,default    # both, in that order
```

`WORKER_QUEUES=images` in the environment does the same. Its startup log lists the queues it serves, and the dashboard's **Workers** tab shows them too.

### Priorities

Within its queues, a worker shares its turns between priorities by weight. With the default `PRIORITY_WEIGHTS=6,3,1`, out of every 10 jobs it takes while all three priorities have work waiting, 6 are `high`, 3 `default` and 1 `low`, spread out (high, default, high, low, high, …). When a priority has nothing waiting, its turn goes to the next one, so with only one kind of work waiting every turn takes it.

| `PRIORITY_WEIGHTS` | Behaviour |
|---|---|
| `6,3,1` (default) | High jobs go first most of the time; default and low jobs still move while high ones keep arriving |
| `10,3,1` | Even more in favour of high |
| `1,1,1` | Priorities only break ties |
| `strict` | Every `high` job first, then `default`, then `low` (the behaviour before 0.5). A steady stream of high jobs keeps the others waiting forever. |

Between queues a worker takes turns for every priority separately, so each queue gets the same share of `high`, `default` and `low` turns: a queue with 10,000 waiting jobs can't starve one with 3, whatever the number of queues. Use the same `PRIORITY_WEIGHTS` everywhere; the dashboard warns when workers differ.

### Pausing a queue

Pause a queue to stop workers from starting new jobs from it, for example while a service its tasks call is down:

- in the dashboard, the pause button next to the queue in the **Queues** row (operators and admins);
- `POST /queues/emails/pause` and `POST /queues/emails/resume`;
- `await app.pause_queue("emails")` and `await app.resume_queue("emails")`.

It takes effect at once. Jobs already running finish normally. New jobs can still be queued and wait; scheduled jobs and retries keep arriving. Cron runs of tasks on a paused queue are **skipped** (logged, and counted in `pytaskq:cron:skipped`), so a day-long pause doesn't release 1,440 runs of an every-minute task at once when you resume. Resume it, and idle workers start on it within a couple of seconds. The pause is stored in Redis, so it survives restarts and applies to every worker, until someone resumes the queue.

### Example: a dedicated image worker

```bash
# Machine A (small): emails and everything else
pytaskq worker --app tasks:queue --queues default

# Machine B (8 cores): only image work
pytaskq worker --app tasks:queue --queues images --cpu-workers 7
```

Both connect to the same Redis. In Docker Compose, run two worker services with different `command:` lines.

### Capacity is per queue

`QUEUE_CAPACITY` (default 500) applies to **each** queue. When `images` is full, new image jobs sent through the API get `429` ("queue 'images' is full"), while emails keep being accepted.

The limit protects the queue from outside traffic. Jobs your app queues with `.delay()`, cron runs, retries and dead-letter replays are never refused. Due scheduled jobs and retries wait in the schedule until their queue is below the limit, so a large backlog that becomes due at once drains steadily instead of filling the queue in one go. Jobs waiting for a full queue never hold up other queues' scheduled jobs and retries, however many there are; the leader logs once a minute which queues hold jobs back.

### What the dashboard shows

- A **Queues** row under the lifecycle strip shows how many jobs wait in each queue and how many workers serve it, and lets operators pause and resume each one. Click a queue to see its jobs. (Viewers see it once there is more than one queue.)
- A queue with waiting jobs and **no worker** is marked, and *Settings → Performance* says how to start one for it.
- The job details show each job's queue.

### Two apps on one queue

If two different apps both use `default`, a worker of app A will sometimes take a job for app B's task. It doesn't fail the job: it hands it back to the queue for a worker that has the task, and the job runs a little later. When no running worker has the task yet (a rolling deploy where the web app went first), the job checks back less and less often. Either way it waits up to `UNKNOWN_TASK_WAIT` (10 minutes), then fails normally (retries, then dead letters), so it can't loop forever. *Settings → Performance* shows how often it happened. Only workers count as having a task: the dashboard's `--app` doesn't.

It's wasted work, though. **Give each app its own queue**, for example `PyTaskQ(default_queue="emails")`.

### Upgrading from 0.3

The `default` queue keeps the Redis key names older versions used (`queue:high`, `queue:default`, `queue:low`), so jobs already waiting are picked up after an upgrade. Other queues use `queue:<name>:<priority>`. A worker from 0.3 only serves `default`.

## Scheduled tasks and time zones

```python
@queue.task(cron="0 9 * * 1-5")                       # 09:00 UTC on weekdays
def morning_report(): ...

@queue.task(cron="0 9 * * 1-5", tz="Asia/Kolkata")    # 09:00 India time
def india_digest(): ...
```

- Schedules are read in **UTC** unless the task sets `tz=`. Set `CRON_TIMEZONE=Europe/London` to change the default for every task. The machine's own clock setting no longer matters, so every worker agrees.
- Time zone names come from the IANA database (`Europe/London`, `America/New_York`, `Asia/Kolkata`). A misspelt name is an error when the task is defined, or when the worker starts for `CRON_TIMEZONE`, not a silent schedule that never runs.
- **Daylight saving:**
  - When clocks go back, a time like 01:30 happens twice. The job runs once.
  - When clocks go forward, a time that's skipped (01:30 in London on the last Sunday of March) runs at the moment the clocks jump.
  - Interval schedules (`*/30 * * * *`, `0 * * * *`) in a zone with daylight saving also run once per local time, so they skip the repeated hour when clocks go back. Use `tz="UTC"` (the default) for schedules that must run every N minutes without a gap.
- Each tick runs once across all workers, and a cron job goes onto its task's queue.

`pytaskq tasks --app tasks:queue` lists each task's queue and, for cron tasks, its time zone.

### Runs that take longer than the interval

A run that hasn't finished when the next tick comes **overlaps** it: both run (a warning in the log says so, at most once an hour). For syncs, reports and cleanups that sometimes run long, skip ticks instead:

```python
@queue.task(cron="*/5 * * * *", overlap=False)
def sync_inventory(): ...
```

With `overlap=False` a tick is skipped (and counted, like ticks skipped for a paused queue) while the previous run is still waiting or running. `max_concurrency=1` isn't the same: it queues the extra runs, so they all still run, one after another.

### Missed runs and catch-up

The workers' leader queues each tick within about 10 seconds of its time. If no worker is running when a tick is due (every worker stopped, a long deploy, Redis down), that tick is **missed**. When a worker is back:

- the log says which runs were missed: *"[Cron] nightly_report missed 3 run(s) between … and …: no worker was running the schedule then."*;
- *Settings → Performance* shows it for a week, and Prometheus counts it (`pytaskq_cron_missed_runs_total`);
- with catch-up on (the default), **the latest missed tick runs once, now**, with a note on the job saying so. Earlier missed ticks don't run: an every-minute task doesn't run 600 times after a 10-hour outage.

Turn catch-up off where a late run would be wrong (a "good morning" message at 3 pm):

```python
@queue.task(cron="0 9 * * *", catch_up=False)
def morning_message(): ...
```

or for every task with `CRON_CATCH_UP=false`. A task that is new starts counting from now: ticks before PyTaskQ first saw it never count as missed.

Run at least two workers so one can take over the schedule when the other stops.

**Changed in 0.4:** schedules used each worker's local time before. If your servers weren't on UTC and you relied on that, add `tz=` or set `CRON_TIMEZONE`.

## Large results

A task's return value is stored in Redis for `RESULT_TTL` (a day), so very large results waste memory. Results over `MAX_RESULT_BYTES` (default 1 MB, `0` = no limit) aren't stored:

- the job still counts as **succeeded**, because its work is done and running it again would repeat that work;
- its record gets `result_dropped`, explaining the size and the limit, which the dashboard shows in place of the result;
- a webhook (`on_success_url`) receives the same `result_dropped` field.

Store big outputs somewhere else, such as a file or object storage, and return their location.

## How the dashboard counts jobs

The *Queued*, *Running*, *Scheduled* and *Dead letters* numbers are counted from Redis itself on every refresh, not kept as separate counters. Counters drifted after crashes and recoveries and sometimes showed impossible values. *Succeeded* is an all-time total.
