# Queues, workers and schedules

This guide covers how jobs are routed to workers, how to give a kind of work its own workers, how scheduled (cron) tasks pick their time zone, and what happens to large results.

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

### Which queues a worker serves

By default, a worker serves **the queues its own tasks live on**. A worker started with the app above serves `default` and `images`, so it never takes a job it can't run. To choose yourself:

```bash
pytaskq worker --app tasks:queue --queues images            # only images
pytaskq worker --app tasks:queue --queues images,default    # both, in that order
```

`WORKER_QUEUES=images` in the environment does the same. Its startup log lists the queues it serves, and the dashboard's **Workers** tab shows them too.

### Order

A worker takes **every queue's `high` jobs first**, then `default`, then `low`. Between queues at the same priority it takes turns, so a queue with 10,000 waiting jobs can't starve one with 3.

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

The limit protects the queue from outside traffic. Jobs your app queues with `.delay()`, cron runs, retries and dead-letter replays are never refused. Due scheduled jobs and retries wait in the schedule until their queue is below the limit, so a large backlog that becomes due at once drains steadily instead of filling the queue in one go.

### What the dashboard shows

- When you use more than one queue, a **Queues** row under the lifecycle strip shows how many jobs wait in each one and how many workers serve it. Click a queue to see its jobs.
- A queue with waiting jobs and **no worker** is marked, and *Settings → Performance* says how to start one for it.
- The job details show each job's queue.

### Two apps on one queue

If two different apps both use `default`, a worker of app A will sometimes take a job for app B's task. It doesn't fail the job: it hands it back to the queue for a worker that has the task, and the job runs a little later. This continues for up to 10 minutes, then the job fails normally (retries, then dead letters), so it can't loop forever. *Settings → Performance* shows how often it happened.

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
- Each tick runs once across all workers, and a cron job goes onto its task's queue.

`pytaskq tasks --app tasks:queue` lists each task's queue and, for cron tasks, its time zone.

**Changed in 0.4:** schedules used each worker's local time before. If your servers weren't on UTC and you relied on that, add `tz=` or set `CRON_TIMEZONE`.

## Large results

A task's return value is stored in Redis for `RESULT_TTL` (a day), so very large results waste memory. Results over `MAX_RESULT_BYTES` (default 1 MB, `0` = no limit) aren't stored:

- the job still counts as **succeeded**, because its work is done and running it again would repeat that work;
- its record gets `result_dropped`, explaining the size and the limit, which the dashboard shows in place of the result;
- a webhook (`on_success_url`) receives the same `result_dropped` field.

Store big outputs somewhere else, such as a file or object storage, and return their location.

## How the dashboard counts jobs

The *Queued*, *Running*, *Scheduled* and *Dead letters* numbers are counted from Redis itself on every refresh, not kept as separate counters. Counters drifted after crashes and recoveries and sometimes showed impossible values. *Succeeded* is an all-time total.
