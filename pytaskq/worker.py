"""
pytaskq.worker — the task-executing process
============================================

Start one (or many, on many machines) with:

    pytaskq worker --app myproject.tasks:queue

Each worker serves a set of named queues (by default, the queues its own tasks
live on; or `--queues a,b`). It pops jobs in strict priority order (high →
default → low), taking turns between its queues at the same priority, runs them
in a process pool (type="cpu"), a thread pool (type="io") or directly on the
event loop (async def), and records status, timings and results in Redis.
Background loops (only on the elected leader) handle cron schedules, delayed
retries and recovery of tasks from crashed workers.
"""

import asyncio
import functools
import inspect
import json
import logging
import os
import random
import signal
import socket
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone

import croniter
from opentelemetry import trace

from pytaskq.core import queueing
from pytaskq.core import redis_client
from pytaskq.core.config import settings
from pytaskq.schemas.requests import Taskloader, Taskresult  # noqa: F401  (Taskresult kept for API compat)
from pytaskq.tracing import extract_trace_context, init_tracer, inject_trace_context  # noqa: F401  (re-exported)

TASKS = {}
APP_NAME = ""
QUEUES: list[str] = []          # the queues this worker serves (set at startup)
QUEUES_OVERRIDE = ""            # `pytaskq worker --queues a,b` (else WORKER_QUEUES, else its tasks' queues)
HAND_BACK_FOR = 600             # seconds a job keeps being handed back for a worker that has its task


def set_tasks(tasks_dict, app_name: str = ""):
    """Register the tasks this worker may execute (called by the CLI)."""
    global APP_NAME
    TASKS.update(tasks_dict)
    if "_deliver_webhook" not in TASKS:
        # Webhooks must work for every app, even one built from a plain dict.
        from pytaskq.app import _deliver_webhook
        TASKS["_deliver_webhook"] = {"handler": _deliver_webhook, "type": "io"}
    if app_name:
        APP_NAME = app_name


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker")

HOSTNAME = socket.gethostname()
WORKER_ID = f"{HOSTNAME}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
TASK_TIMEOUT = float(os.getenv("TASK_TIMEOUT", str(settings.TASK_TIMEOUT)))
HEARTBEAT_TTL = 30

_TOTAL_CORES = os.cpu_count() or 1
_RESERVED_CORES = int(os.getenv("WORKER_RESERVED_CORES", "1"))
CPU_WORKERS = int(os.getenv("WORKER_CPU_WORKERS", "0")) or max(1, _TOTAL_CORES - _RESERVED_CORES)
IO_WORKERS = int(os.getenv("WORKER_IO_WORKERS", "0")) or max(4, _TOTAL_CORES * 4)
CONCURRENCY = CPU_WORKERS + IO_WORKERS


class TaskTimeoutError(Exception):
    """Raised when a task exceeds its timeout."""


class UnknownTaskError(Exception):
    """Raised when a payload names a task this worker does not know."""


def resolve_queues(explicit: str = "") -> list[str]:
    """
    The queues this worker serves, in order.

    `--queues` / WORKER_QUEUES when given; otherwise every queue one of this
    worker's own tasks lives on (so it never takes jobs it can't run), or just
    "default" for an app without tasks.
    """
    raw = explicit or settings.WORKER_QUEUES
    if raw.strip():
        names = [queueing.check_queue_name(n.strip()) for n in raw.split(",") if n.strip()]
    else:
        names = sorted({(entry.get("queue") or queueing.DEFAULT_QUEUE)
                        for name, entry in TASKS.items() if not name.startswith("_")},
                       key=lambda n: (n != queueing.DEFAULT_QUEUE, n)) or [queueing.DEFAULT_QUEUE]
    return list(dict.fromkeys(names))


def pop_keys(queues: list[str], turn: int = 0) -> list[str]:
    """
    The lists to pop from, in order: every queue's high list, then default, then low.

    Within one priority the starting queue rotates with `turn`, so a busy queue
    can't starve the others.
    """
    if not queues:
        return []
    start = turn % len(queues)
    rotated = queues[start:] + queues[:start]
    return [queueing.queue_key(p, q) for p in queueing.PRIORITIES for q in rotated]


def cron_zone(name: str):
    """A tzinfo for a cron time zone name ("UTC" works even without a time zone database)."""
    if not name or name.strip().upper() == "UTC":
        return timezone.utc
    from zoneinfo import ZoneInfo
    return ZoneInfo(name.strip())


shutdown_event = asyncio.Event()
active_tasks = set()
tracer = init_tracer("pytaskq-worker")

FENCE_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    redis.call('hset', KEYS[2], 'task_id', ARGV[2], 'status', ARGV[3], 'result', ARGV[4])
    redis.call('expire', KEYS[2], tonumber(ARGV[5]) or 86400)
    return 1
else
    return 0
end
"""

# On timeout, invalidate this attempt — but only if it still owns the job.
# If the sweeper already handed the job to another worker, bumping the fence
# would wrongly make THIS attempt's retry the current one. Returns -1 then.
TIMEOUT_FENCE_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    local token = redis.call('incr', KEYS[1])
    redis.call('expire', KEYS[1], 86400)
    return token
end
return -1
"""

# Move one due job from the delayed set onto its queue list.  KEYS: delayed zset, list
REQUEUE_SCRIPT = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[2], ARGV[1])
    return 1
end
return 0
"""

# Queue a cron job once per tick across the fleet.  KEYS: tick lock, list, queue-name set
CRON_ENQUEUE_SCRIPT = """
local acquired = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 2592000)
if acquired then
    redis.call('LPUSH', KEYS[2], ARGV[1])
    redis.call('SADD', KEYS[3], ARGV[2])
    return 1
end
return 0
"""

PRIORITY_POP_SCRIPT = """
local dest = KEYS[1]
for i = 2, #KEYS do
    local val = redis.call('RPOPLPUSH', KEYS[i], dest)
    if val then
        return val
    end
end
return nil
"""


async def is_leader() -> bool:
    try:
        acquired = await redis_client.r.set("pytaskq:leader_lock", WORKER_ID, nx=True, ex=20)
        if acquired:
            return True

        current_leader = await redis_client.r.get("pytaskq:leader_lock")
        if current_leader == WORKER_ID:
            await redis_client.r.expire("pytaskq:leader_lock", 20)
            return True

        return False
    except Exception as e:
        logger.warning(f"[Leader Election] Redis error: {e}")
        return False


async def cron_scheduler():
    while True:
        if not await is_leader():
            await asyncio.sleep(10)
            continue

        await run_due_cron_jobs()

        await asyncio.sleep(10)


async def run_due_cron_jobs(now: float = None) -> int:
    """Queue every cron task whose latest tick was in the last minute (once per tick, fleet-wide).

    Schedules are read in the task's `tz` (else CRON_TIMEZONE, UTC by default),
    so "0 9 * * *" means 09:00 in that zone on every machine, whatever the
    machine's own clock is set to, and every worker computes the same tick.
    """
    now = time.time() if now is None else now
    queued = 0
    for task_name, task_info in list(TASKS.items()):
        cron_expr = task_info.get("cron")
        if not cron_expr:
            continue
        try:
            zone_name = task_info.get("tz") or settings.CRON_TIMEZONE
            zone = cron_zone(zone_name)
            local_now = datetime.fromtimestamp(now, zone)
            prev_tick = croniter.croniter(cron_expr, local_now).get_prev(datetime)
            if now - prev_tick.timestamp() >= 60:
                continue
            # One run per LOCAL clock time: when clocks go back, 01:30 happens twice,
            # and the second one finds the first one's lock (standard cron runs once too).
            lock = f"cron_lock:{task_name}:{zone_name}:{prev_tick.strftime('%Y-%m-%dT%H:%M')}"
            queue = task_info.get("queue") or queueing.DEFAULT_QUEUE
            payload = queueing.new_payload(task_name, priority=task_info.get("priority") or "default", queue=queue)
            enqueued = await redis_client.r.eval(
                CRON_ENQUEUE_SCRIPT, 3,
                lock, queueing.queue_key(payload["priority"], queue), queueing.QUEUES_KEY,
                queueing.dumps_payload(payload), queue,
            )
            if enqueued:
                queued += 1
                await queueing.record(redis_client.r, payload)
                logger.info(f"[Cron] Queued {task_name} for {prev_tick.isoformat()}")
        except Exception as e:
            logger.error(f"[Cron] Could not schedule {task_name}: {e}")
    return queued


async def retry_scheduler():
    while True:
        if not await is_leader():
            await asyncio.sleep(5)
            continue
        try:
            await _promote_due_tasks()
        except Exception as e:
            logger.error(f"[Retry Scheduler] Redis error: {e}")
        await asyncio.sleep(1)


async def _promote_due_tasks(now: float = None) -> int:
    """Move delayed/retry tasks whose time has come onto their priority queue."""
    now = time.time() if now is None else now
    ready_tasks = await redis_client.r.zrangebyscore("delayed_tasks", "-inf", now)
    moved = 0
    for i, task_json in enumerate(ready_tasks):
        if i > 0 and i % 100 == 0:
            await redis_client.r.expire("pytaskq:leader_lock", 20)
        try:
            task_data = json.loads(task_json)
            priority = task_data.get("priority", "default")
            requeued = await redis_client.r.eval(
                REQUEUE_SCRIPT, 2,
                "delayed_tasks", queueing.queue_key(priority, queueing.payload_queue(task_data)),
                task_json,
            )
            if requeued:
                moved += 1
                task_id = task_data.get("task_id")
                if task_id and task_data.get("retry_count", 0) == 0:
                    # A countdown task is now simply waiting in its queue
                    if await redis_client.r.hget(queueing.task_key(task_id), "status") == queueing.SCHEDULED:
                        await redis_client.r.hset(queueing.task_key(task_id), "status", queueing.QUEUED)
                logger.info(f"[Scheduler] Queued task {task_id} (retry #{task_data.get('retry_count', 0)})")
        except json.JSONDecodeError:
            logger.error("[Scheduler] Malformed task found in delayed_tasks. Discarding.")
            await redis_client.r.zrem("delayed_tasks", task_json)
        except Exception as e:
            logger.error(f"[Scheduler] Error processing task: {e}")
    return moved


def _worker_info() -> dict:
    return {
        "worker_id": WORKER_ID,
        "hostname": HOSTNAME,
        "pid": os.getpid(),
        "app": APP_NAME,
        "cpu_workers": CPU_WORKERS,
        "io_workers": IO_WORKERS,
        "tasks": len([t for t in TASKS if not t.startswith("_")]),
        "queues": ",".join(QUEUES),
    }


async def _publish_registry():
    from pytaskq.app import describe_tasks
    if TASKS:
        await queueing.publish_registry(redis_client.r, describe_tasks(TASKS))


async def heartbeat():
    last_registry = 0.0
    while True:
        try:
            now = time.time()
            async with redis_client.r.pipeline(transaction=True) as pipe:
                pipe.zadd("active_workers", {WORKER_ID: now + HEARTBEAT_TTL})
                pipe.hset(f"worker:{WORKER_ID}", mapping={"last_seen": now, "active": len(active_tasks),
                                                          "queues": ",".join(QUEUES)})
                pipe.expire(f"worker:{WORKER_ID}", HEARTBEAT_TTL * 4)
                if QUEUES:
                    pipe.sadd(queueing.QUEUES_KEY, *QUEUES)
                await pipe.execute()
            if now - last_registry > 60:
                await _publish_registry()
                last_registry = now
            from pytaskq.core import pressure
            await pressure.flush(redis_client.r)    # hand-backs counted here show in Settings → Performance
            await asyncio.sleep(10)
        except Exception as e:
            logger.warning(f"[Heartbeat] Redis unreachable: {e}. Retrying in 5s")
            await asyncio.sleep(5)


async def zombie_sweeper():
    while True:
        if not await is_leader():
            await asyncio.sleep(5)
            continue
        try:
            await _sweep_dead_workers()
        except Exception as e:
            logger.error(f"[Sweeper] Redis error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        await asyncio.sleep(15)


async def _sweep_dead_workers() -> int:
    """Re-queue in-flight tasks of workers whose heartbeat expired."""
    expired = await redis_client.r.zrangebyscore("active_workers", "-inf", time.time())
    total = 0
    for worker_id in expired:
        tasks_recovered = 0
        while True:
            tasks = await redis_client.r.lrange(f"processing_queue:{worker_id}", -1, -1)
            if not tasks:
                break

            task = tasks[0]
            if tasks_recovered > 0 and tasks_recovered % 100 == 0:
                await redis_client.r.expire("pytaskq:leader_lock", 20)

            try:
                task_data = json.loads(task)
                task_data["fence_token"] = int(task_data.get("fence_token", 0)) + 1
                task_id = task_data.get("task_id")
                new_raw = json.dumps(task_data)

                async with redis_client.r.pipeline(transaction=True) as pipe:
                    pipe.lrem(f"processing_queue:{worker_id}", count=1, value=task)
                    pipe.set(f"fence:{task_id}", task_data["fence_token"], ex=86400)
                    pipe.lpush(queueing.queue_key("high", queueing.payload_queue(task_data)), new_raw)
                    if task_id and await redis_client.r.exists(queueing.task_key(task_id)):
                        pipe.hset(queueing.task_key(task_id), mapping={
                            "status": queueing.QUEUED, "priority": "high", "payload": new_raw,
                            "note": f"Recovered from dead worker {worker_id}",
                        })
                    await pipe.execute()
            except json.JSONDecodeError:
                logger.error(f"[Sweeper] Malformed task found in {worker_id} processing queue. Discarding.")
                await redis_client.r.lrem(f"processing_queue:{worker_id}", count=1, value=task)

            tasks_recovered += 1

        await redis_client.r.zrem("active_workers", worker_id)
        await redis_client.r.delete(f"worker:{worker_id}")
        total += tasks_recovered
        logger.info(f"[Sweeper] Recovered {tasks_recovered} task(s) from dead worker: {worker_id}")
    return total


def _resolve(value, entry, key, default):
    if value is not None:
        return value
    if entry and entry.get(key) is not None:
        return entry[key]
    return default


async def _record_failure(r, tasks, task_json, task_id, error, max_retries) -> str:
    """
    Schedule a retry or dead-letter a failed attempt — unless the attempt is stale.

    An attempt is stale when the job was handed to another worker meanwhile (the
    sweeper thought this worker was dead and bumped the job's fence token). The
    newer attempt owns the job, so this one must not schedule a second copy or
    dead-letter it. The fence is checked and the retry/DLQ written atomically
    (WATCH), so a takeover can't slip in between.
    """
    from redis.exceptions import WatchError
    fence_key = f"fence:{task_id}"
    finished_at = time.time()
    dead = tasks.retry_count >= max_retries
    async with r.pipeline(transaction=True) as pipe:
        try:
            await pipe.watch(fence_key)
            current = await pipe.get(fence_key)
            if current is not None and current != str(tasks.fence_token):
                await pipe.reset()
                logger.warning(f"Task {task_id} attempt failed after the job was handed to another worker; "
                               f"ignoring it ({error}).")
                return "Discarded"
            pipe.multi()
            pipe.incr("stats:failed")
            pipe.hincrby(f"worker:{WORKER_ID}", "failed", 1)
            if dead:
                dlq_entry = json.loads(task_json)
                dlq_entry["fence_token"] = tasks.fence_token
                dlq_entry["error"] = error
                dlq_entry["failed_at"] = finished_at
                pipe.lpush("dlq", json.dumps(dlq_entry))
                pipe.hset(queueing.task_key(task_id), mapping={
                    "task_id": task_id,
                    "status": queueing.DEAD_LETTER,
                    "finished_at": finished_at,
                    "error": f"Failed after {tasks.retry_count} retries. Last error: {error}",
                })
                pipe.expire(queueing.task_key(task_id), settings.RESULT_TTL)
            else:
                tasks.retry_count += 1
                base, max_delay = 1.0, 30.0
                delay = min(max_delay, random.uniform(base, (tasks.prev_delay or base) * 3))
                tasks.prev_delay = delay
                retry_raw = json.dumps(tasks.model_dump())
                pipe.zadd("delayed_tasks", {retry_raw: finished_at + delay})
                pipe.hset(queueing.task_key(task_id), mapping={
                    "task_id": task_id,
                    "status": queueing.RETRY_SCHEDULED,
                    "retry_count": tasks.retry_count,
                    "next_retry_at": finished_at + delay,
                    "payload": retry_raw,
                    "error": f"{error}. Retry {tasks.retry_count}/{max_retries} in {delay:.1f}s.",
                })
                pipe.expire(queueing.task_key(task_id), int(settings.RESULT_TTL + delay))
            await pipe.execute()
        except WatchError:
            logger.warning(f"Task {task_id} was handed to another worker while recording its failure; ignoring it.")
            return "Discarded"
    if dead:
        logger.error(f"[DLQ] Task {task_id} failed after {tasks.retry_count} retries. Moving to DLQ. ({error})")
        return "DeadLetter"
    logger.error(f"Task {task_id} failed: {error}. Retry {tasks.retry_count}/{max_retries} scheduled.")
    return "RetryScheduled"


def _storable_result(result) -> tuple[str, str]:
    """(what to store as the result, why it was dropped or "").

    Results over MAX_RESULT_BYTES aren't stored: a 50 MB return value would sit
    in Redis for RESULT_TTL. The job still counts as a success (its work is done;
    running it again would repeat that work), and the reason is recorded.
    """
    encoded = queueing.encode_result(result)
    limit = settings.MAX_RESULT_BYTES
    if limit and limit > 0:
        size = len(encoded.encode("utf-8", "replace"))
        if size > limit:
            return "", (f"The result was {size:,} bytes, over MAX_RESULT_BYTES ({limit:,}), so it wasn't stored. "
                        f"Save large output elsewhere (a file, S3) and return its location instead.")
    return encoded, ""


async def _hand_back(r, tasks, task_json, task_id) -> bool:
    """
    This worker doesn't have the job's task, but another live worker advertises it
    (two apps sharing one queue). Put the job back on its queue for that worker,
    without counting a retry. Whichever worker pops first gets the job, so it may
    take several hand-backs; this keeps going for HAND_BACK_FOR seconds, then the
    job fails normally (retries, then dead letters) so it can't loop forever.
    Settings → Performance says to give each app its own queue.
    """
    now = time.time()
    since = tasks.handed_back_since or now
    if now - since > HAND_BACK_FOR:
        return False
    spec, _ = await queueing.registry_lookup(r, tasks.task_name)
    if spec is None:
        return False
    payload = json.loads(task_json)
    payload["bounces"] = tasks.bounces + 1
    payload["handed_back_since"] = since
    raw = json.dumps(payload)
    due = now + random.uniform(0.5, 2.0)   # via the delayed set, so it doesn't bounce straight back here
    async with r.pipeline(transaction=True) as pipe:
        pipe.zadd("delayed_tasks", {raw: due})
        pipe.hset(queueing.task_key(task_id), mapping={
            "status": queueing.QUEUED, "payload": raw,
            "note": f"Waiting for a worker that has task '{tasks.task_name}' (worker {WORKER_ID} doesn't). "
                    f"Give each app its own queue to avoid this.",
        })
        await pipe.execute()
    from pytaskq.core import pressure
    pressure.record("handed_back", f"'{tasks.task_name}' on queue '{tasks.queue}' (worker {WORKER_ID} lacks it)")
    return True


async def handle_task(task_json, sem, loop, process_pool, thread_pool):
    r = redis_client.r
    try:
        try:
            tasks = Taskloader.model_validate_json(task_json)
            task_id = tasks.task_id
            my_token = tasks.fence_token
        except Exception as e:
            logger.error(f"Task validation error: {e}")
            try:
                task_id = str(json.loads(task_json).get("task_id") or "Unknown")
            except Exception:
                task_id = "Unknown"
            async with r.pipeline(transaction=True) as pipe:
                pipe.hset(queueing.task_key(task_id), mapping={
                    "task_id": task_id,
                    "status": queueing.FAILED,
                    "error": f"Invalid task payload: {e}",
                    "finished_at": time.time(),
                })
                pipe.expire(queueing.task_key(task_id), settings.RESULT_TTL)
                pipe.incr("stats:failed")
                await pipe.execute()
            return

        entry = TASKS.get(tasks.task_name)
        max_retries = int(_resolve(tasks.max_retries, entry, "max_retries", settings.MAX_RETRIES))
        timeout = float(_resolve(tasks.timeout, entry, "timeout", TASK_TIMEOUT))

        parent_ctx = extract_trace_context(getattr(tasks, "trace_carrier", {}))
        with tracer.start_as_current_span(
            f"run {tasks.task_name}",
            context=parent_ctx,
            kind=trace.SpanKind.CONSUMER,
            attributes={
                "task.id": task_id,
                "task.name": tasks.task_name,
                "task.retry_count": tasks.retry_count,
                "task.priority": tasks.priority,
                "worker.id": WORKER_ID,
            },
        ) as span:
            started_at = time.time()
            try:
                if entry is None and await _hand_back(r, tasks, task_json, task_id):
                    span.set_attribute("task.status", "HandedBack")
                    return
                if entry is None:
                    known = ", ".join(sorted(t for t in TASKS if not t.startswith("_"))) or "none"
                    raise UnknownTaskError(
                        f"Task '{tasks.task_name}' is not registered on worker {WORKER_ID}. "
                        f"Registered tasks: {known}"
                    )
                func = entry["handler"]
                task_type = entry.get("type", "cpu")

                logger.info(f"Executing task {task_id} ({tasks.task_name})")
                async with r.pipeline(transaction=True) as pipe:
                    pipe.hset(queueing.task_key(task_id), mapping={
                        "task_id": task_id,
                        "task_name": tasks.task_name,
                        "status": queueing.PROCESSING,
                        "started_at": started_at,
                        "worker_id": WORKER_ID,
                        "retry_count": tasks.retry_count,
                    })
                    pipe.expire(queueing.task_key(task_id), settings.RESULT_TTL)
                    pipe.publish(queueing.EVENTS_CHANNEL, "update")
                    await pipe.execute()

                args, kwargs = tasks.args, tasks.kwargs
                if inspect.iscoroutinefunction(func):
                    exec_future = asyncio.ensure_future(func(*args, **kwargs))
                else:
                    call = functools.partial(func, *args, **kwargs) if kwargs else func
                    call_args = () if kwargs else tuple(args)
                    pool = process_pool if (task_type == "cpu" and process_pool is not None) else thread_pool
                    exec_future = loop.run_in_executor(pool, call, *call_args)

                if timeout > 0:
                    try:
                        result = await asyncio.wait_for(exec_future, timeout=timeout)
                    except asyncio.TimeoutError:
                        # Invalidate this attempt; the retry carries the new token so
                        # ITS result is accepted while any late write from this one is not.
                        new_token = int(await r.eval(TIMEOUT_FENCE_SCRIPT, 1, f"fence:{task_id}", str(my_token)))
                        if new_token >= 0:
                            tasks.fence_token = new_token
                        raise TaskTimeoutError(
                            f"Task exceeded {timeout:g}s limit. Execution abandoned."
                        ) from None
                else:
                    result = await exec_future

                encoded, dropped = _storable_result(result)
                saved = await r.eval(
                    FENCE_SCRIPT, 2,
                    f"fence:{task_id}", queueing.task_key(task_id),
                    str(my_token), task_id, queueing.SUCCESS, encoded,
                    int(settings.RESULT_TTL),
                )

                if saved:
                    finished_at = time.time()
                    async with r.pipeline(transaction=True) as pipe:
                        pipe.hset(queueing.task_key(task_id), mapping={
                            "finished_at": finished_at,
                            "duration_ms": round((finished_at - started_at) * 1000, 1),
                            "error": "",
                            **({"result_dropped": dropped} if dropped else {}),
                        })
                        pipe.expire(queueing.task_key(task_id), settings.RESULT_TTL)
                        pipe.incr("stats:completed")
                        pipe.hincrby(f"worker:{WORKER_ID}", "processed", 1)
                        await pipe.execute()
                    span.set_attribute("task.status", "Success")
                    logger.info(f"Task {task_id} completed successfully.")
                    # ── Webhook delivery ──────────────────────────────────────
                    # Travels through the queue like any other task, so network
                    # blips are retried automatically.
                    if dropped:
                        logger.warning(f"Task {task_id} succeeded but its result wasn't stored: {dropped}")
                    if tasks.on_success_url:
                        webhook = queueing.new_payload(
                            "_deliver_webhook",
                            [tasks.on_success_url, task_id, tasks.task_name, encoded]
                            + ([dropped] if dropped else []),
                            queue=tasks.queue,   # a worker serving this queue certainly has the built-in task
                        )
                        await queueing.enqueue(r, webhook)
                        logger.info(f"Enqueued webhook delivery for task {task_id} → {tasks.on_success_url}")
                else:
                    span.set_attribute("task.status", "Discarded")
                    span.set_attribute("task.discard_reason", "fence_token_stale")
                    logger.warning(f"Task {task_id} discarded (stale fence token).")

            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                span.set_status(trace.StatusCode.ERROR, str(e))
                span.record_exception(e)
                outcome = await _record_failure(r, tasks, task_json, task_id, error, max_retries)
                span.set_attribute("task.status", outcome)
                if outcome == "Discarded":
                    span.set_attribute("task.discard_reason", "fence_token_stale")

    finally:
        try:
            async with r.pipeline(transaction=True) as pipe:
                pipe.lrem(f"processing_queue:{WORKER_ID}", count=1, value=task_json)
                pipe.publish(queueing.EVENTS_CHANNEL, "update")
                await pipe.execute()
        except Exception as e:
            logger.error(f"[Cleanup] Redis error: {e}")
        finally:
            sem.release()


async def _recover_own_tasks():
    """On boot, put back anything this worker id left in its processing queue (onto its own queue)."""
    key = f"processing_queue:{WORKER_ID}"
    while True:
        leftovers = await redis_client.r.lrange(key, -1, -1)
        if not leftovers:
            break
        leftover = leftovers[0]
        try:
            target = queueing.queue_key("high", queueing.payload_queue(json.loads(leftover)))
        except ValueError:
            target = queueing.queue_key("high")
        async with redis_client.r.pipeline(transaction=True) as pipe:
            pipe.lrem(key, 1, leftover)
            pipe.lpush(target, leftover)
            await pipe.execute()
        logger.info(f"Recovered crashed task on startup: {leftover}")


def check_cron_zones() -> None:
    """Fail at startup (not silently every 10 s) when a cron time zone doesn't exist."""
    names = {settings.CRON_TIMEZONE} | {e.get("tz") for e in TASKS.values() if e.get("cron") and e.get("tz")}
    for name in names:
        try:
            cron_zone(name)
        except Exception:
            raise SystemExit(f"Unknown time zone {name!r} for cron schedules. Use a name like 'UTC', "
                             f"'Europe/London' or 'Asia/Kolkata' (CRON_TIMEZONE or the task's tz=).") from None


async def consumer_task():
    global QUEUES
    concurrency = CPU_WORKERS + IO_WORKERS
    QUEUES = resolve_queues(QUEUES_OVERRIDE)
    check_cron_zones()
    logger.info(
        f"Worker {WORKER_ID}: {_TOTAL_CORES} CPU cores detected. "
        f"CPU processes={CPU_WORKERS}, IO threads={IO_WORKERS}, max concurrent tasks={concurrency}"
    )
    user_tasks = sorted(t for t in TASKS if not t.startswith("_"))
    logger.info(f"Registered tasks ({len(user_tasks)}): {', '.join(user_tasks) or 'none'}")
    logger.info(f"Serving queues: {', '.join(QUEUES)}")
    elsewhere = sorted({e.get("queue") or queueing.DEFAULT_QUEUE for n, e in TASKS.items()
                        if not n.startswith("_")} - set(QUEUES))
    if elsewhere:
        logger.warning(f"This worker has tasks on queues it doesn't serve ({', '.join(elsewhere)}); "
                       f"another worker must serve them, or their jobs will wait.")

    process_pool = ProcessPoolExecutor(max_workers=CPU_WORKERS)
    thread_pool = ThreadPoolExecutor(max_workers=IO_WORKERS)
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(concurrency)

    def _signal_handler():
        logger.info("Shutdown signal received. Finishing running tasks...")
        shutdown_event.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:  # Windows
        signal.signal(signal.SIGINT, lambda s, f: loop.call_soon_threadsafe(_signal_handler))
        signal.signal(signal.SIGTERM, lambda s, f: loop.call_soon_threadsafe(_signal_handler))

    while not shutdown_event.is_set():
        try:
            await _recover_own_tasks()
            await redis_client.r.hset(f"worker:{WORKER_ID}", mapping={**_worker_info(), "started_at": time.time()})
            await redis_client.r.sadd(queueing.QUEUES_KEY, *QUEUES)
            await _publish_registry()
            break
        except Exception as e:
            logger.error(f"[Startup] Redis unavailable ({e.__class__.__name__}). Retrying in 5s...")
            await asyncio.sleep(5)

    background = [
        asyncio.create_task(cron_scheduler()),
        asyncio.create_task(retry_scheduler()),
        asyncio.create_task(heartbeat()),
        asyncio.create_task(zombie_sweeper()),
    ]

    logger.info(f"Worker {WORKER_ID} started and ready to process tasks.")

    idle_delay = 0.05
    max_idle_delay = 1.0
    turn = 0

    while not shutdown_event.is_set():
        try:
            await sem.acquire()
            if shutdown_event.is_set():     # stopping: don't start another job
                sem.release()
                break
            keys = pop_keys(QUEUES, turn)
            turn += 1
            task_json = await redis_client.r.eval(
                PRIORITY_POP_SCRIPT, 1 + len(keys), f"processing_queue:{WORKER_ID}", *keys,
            )

            if not task_json:
                sem.release()
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=idle_delay)
                except asyncio.TimeoutError:
                    pass
                idle_delay = min(idle_delay * 1.5, max_idle_delay)
                continue

            idle_delay = 0.05
        except Exception as e:
            sem.release()
            logger.error(f"[Main loop] Redis error: {e.__class__.__name__}. Backing off 5s...")
            await asyncio.sleep(5)
            continue

        task = asyncio.create_task(
            handle_task(task_json, sem, loop, process_pool, thread_pool)
        )
        active_tasks.add(task)
        task.add_done_callback(active_tasks.discard)

    if active_tasks:
        logger.info(f"Waiting for {len(active_tasks)} active tasks to complete...")
        await asyncio.gather(*active_tasks, return_exceptions=True)

    for bg in background:
        bg.cancel()
    try:
        await redis_client.r.zrem("active_workers", WORKER_ID)
        await redis_client.r.delete(f"worker:{WORKER_ID}")
    except Exception:
        pass

    thread_pool.shutdown(wait=True)
    process_pool.shutdown(wait=True)
    await redis_client.r.aclose()

    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()
    logger.info(f"Worker {WORKER_ID} stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(consumer_task())
