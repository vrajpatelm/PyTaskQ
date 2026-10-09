import json
import time
import uuid
import asyncio
import signal
import os
import logging
import random
from typing import Any, List
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from opentelemetry import trace
import croniter
from datetime import datetime

from pytaskq.schemas.requests import Taskresult, Taskloader
from pytaskq.tracing import init_tracer, extract_trace_context, inject_trace_context
from pytaskq.core import redis_client

TASKS = {}

def set_tasks(tasks_dict):
    global TASKS
    TASKS.update(tasks_dict)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("worker")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WORKER_ID = str(uuid.uuid4())
TASK_TIMEOUT = float(os.getenv("TASK_TIMEOUT", "300"))

_TOTAL_CORES = os.cpu_count() or 1
_RESERVED_CORES = int(os.getenv("WORKER_RESERVED_CORES", "1"))
CPU_WORKERS = max(1, _TOTAL_CORES - _RESERVED_CORES)
IO_WORKERS = max(4, _TOTAL_CORES * 4)
CONCURRENCY = CPU_WORKERS + IO_WORKERS

class TaskTimeoutError(Exception):
    """Raised when a task exceeds TASK_TIMEOUT."""
    pass

shutdown_event = asyncio.Event()
active_tasks = set()
tracer = init_tracer("pytaskq-worker")

FENCE_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    redis.call('hset', KEYS[2], 'task_id', ARGV[2], 'status', ARGV[3], 'result', ARGV[4])
    redis.call('expire', KEYS[2], 86400)
    return 1
else
    return 0
end
"""

REQUEUE_SCRIPT = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[2], ARGV[1])
    redis.call('DECR', KEYS[3])
    redis.call('INCR', KEYS[4])
    return 1
end
return 0
"""

CRON_ENQUEUE_SCRIPT = """
local acquired = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 2592000)
if acquired then
    redis.call('LPUSH', KEYS[2], ARGV[1])
    redis.call('INCR', KEYS[3])
    return 1
end
return 0
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
            
        try:
            now = datetime.now()
            for task_name, task_info in TASKS.items():
                cron_expr = task_info.get("cron")
                if not cron_expr:
                    continue
                
                itr = croniter.croniter(cron_expr, now)
                prev_tick = itr.get_prev(datetime)
                tick_ts = int(prev_tick.timestamp())
                
                if (now.timestamp() - tick_ts) < 60:
                    lock_key = f"cron_lock:{task_name}:{tick_ts}"
                    task_id = str(uuid.uuid4())
                    
                    task_payload = {
                        "task_name": task_name,
                        "args": [],
                        "task_id": task_id,
                        "retry_count": 0,
                        "priority": "default",
                        "fence_token": 0,
                        "trace_carrier": {}
                    }
                    
                    enqueued = await redis_client.r.eval(
                        CRON_ENQUEUE_SCRIPT, 3,
                        lock_key, "queue:default", "stats:pending",
                        json.dumps(task_payload)
                    )
                    
                    if enqueued:
                        logger.info(f"[Cron] Scheduled {task_name} for tick {tick_ts}")
                        
        except Exception as e:
            logger.error(f"[Cron Scheduler] Error evaluating cron schedules: {e}")
            
        await asyncio.sleep(10)

async def retry_scheduler():
    while True:
        if not await is_leader():
            await asyncio.sleep(5)
            continue
            
        now = time.time()
        ready_tasks = await redis_client.r.zrangebyscore("delayed_tasks", "-inf", now)
        for i, task_json in enumerate(ready_tasks):
            if i > 0 and i % 100 == 0:
                await redis_client.r.expire("pytaskq:leader_lock", 20)
                
            try:
                task_data = json.loads(task_json)
                priority = task_data.get('priority', 'default')
                
                requeued = await redis_client.r.eval(
                    REQUEUE_SCRIPT, 4,
                    "delayed_tasks", f"queue:{priority}",
                    "stats:delayed", "stats:pending",
                    task_json
                )
                
                if requeued:
                    logger.info(f"[Retry Scheduler] Re-queued task {task_data.get('task_id')} (retry #{task_data.get('retry_count')})")
            except json.JSONDecodeError:
                logger.error("[Retry Scheduler] Malformed task found in delayed_tasks. Discarding.")
                await redis_client.r.zrem("delayed_tasks", task_json)
            except Exception as e:
                logger.error(f"[Retry Scheduler] Error processing task: {e}")
        
        await asyncio.sleep(1)  

async def heartbeat():
    while True:
        try:
            await redis_client.r.zadd("active_workers", {WORKER_ID: time.time() + 30})
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
            expired = await redis_client.r.zrangebyscore("active_workers", "-inf", time.time())
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
                        task_data["fence_token"] += 1
                        task_id = task_data.get("task_id")
                        
                        async with redis_client.r.pipeline(transaction=True) as pipe:
                            pipe.lrem(f"processing_queue:{worker_id}", count=1, value=task)
                            pipe.set(f"fence:{task_id}", task_data["fence_token"], ex=86400)
                            pipe.lpush("queue:high", json.dumps(task_data))
                            pipe.incr("stats:pending")
                            pipe.decr("stats:processing")
                            await pipe.execute()
                    except json.JSONDecodeError:
                        logger.error(f"[Sweeper] Malformed task found in {worker_id} processing queue. Discarding.")
                        await redis_client.r.lrem(f"processing_queue:{worker_id}", count=1, value=task)
                    
                    tasks_recovered += 1
                    
                await redis_client.r.zrem("active_workers", worker_id)
                logger.info(f"[Sweeper] Recovered tasks from dead worker: {worker_id}")
        except Exception as e:
            logger.error(f"[Sweeper] Redis error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        await asyncio.sleep(15)

async def handle_task(task_json, sem, loop, process_pool, thread_pool):
    incr_done = False
    try:  
        try:
            tasks = Taskloader.model_validate_json(task_json)
            task_id = tasks.task_id
            my_token = tasks.fence_token
            await redis_client.r.decr("stats:pending")
        except Exception as e:
            logger.error(f"Task validation error: {e}")
            task_id = "Unknown"
            await redis_client.r.hset(f"task:{task_id}", mapping={
                "task_id": task_id,
                "status": "Failed",
                "error": f"JSON Validation Error: {str(e)}"
            })
            return  

        parent_ctx = extract_trace_context(getattr(tasks, 'trace_carrier', {}))
        with tracer.start_as_current_span(
            "execute_task",
            context=parent_ctx,
            attributes={
                "task.id": task_id,
                "task.name": tasks.task_name,
                "task.retry_count": tasks.retry_count,
                "worker.id": WORKER_ID,
            },
        ) as span:
            try:
                entry = TASKS[tasks.task_name]
                func = entry["handler"]
                task_type = entry["type"]
                
                logger.info(f"Executing task {task_id} ({tasks.task_name})")
                await redis_client.r.incr("stats:processing")
                incr_done = True

                if task_type == "cpu":
                    exec_future = loop.run_in_executor(process_pool, func, *tasks.args)
                else:
                    exec_future = loop.run_in_executor(thread_pool, func, *tasks.args)

                if TASK_TIMEOUT > 0:
                    try:
                        result = await asyncio.wait_for(exec_future, timeout=TASK_TIMEOUT)
                    except asyncio.TimeoutError:
                        await redis_client.r.incr(f"fence:{task_id}")
                        await redis_client.r.expire(f"fence:{task_id}", 86400)
                        raise TaskTimeoutError(
                            f"Task exceeded {TASK_TIMEOUT}s limit. Execution abandoned."
                        ) from None
                else:
                    result = await exec_future

                saved = await redis_client.r.eval(
                    FENCE_SCRIPT, 2,
                    f"fence:{task_id}", f"task:{task_id}",
                    str(my_token), task_id, "Success", str(result)
                )
                
                if saved:
                    async with redis_client.r.pipeline(transaction=True) as pipe:
                        pipe.expire(f"task:{task_id}", 86400)
                        pipe.incr("stats:completed")
                        await pipe.execute()
                    span.set_attribute("task.status", "Success")
                    logger.info(f"Task {task_id} completed successfully.")
                    # ── Webhook delivery ──────────────────────────────────────
                    # If the task carried an on_success_url, enqueue a
                    # _deliver_webhook task. It travels through the queue like
                    # any other task, so network blips are retried automatically.
                    if tasks.on_success_url:
                        webhook_payload = json.dumps({
                            "task_name": "_deliver_webhook",
                            "task_id": str(uuid.uuid4()),
                            "args": [tasks.on_success_url, task_id, tasks.task_name, str(result)],
                            "retry_count": 0,
                            "fence_token": 0,
                            "priority": "default",
                        })
                        await redis_client.r.lpush("queue:default", webhook_payload)
                        logger.info(f"Enqueued webhook delivery for task {task_id} → {tasks.on_success_url}")
                else:
                    span.set_attribute("task.status", "Discarded")
                    span.set_attribute("task.discard_reason", "fence_token_stale")
                    logger.warning(f"Task {task_id} discarded (stale fence token).")
                
            except Exception as e:
                logger.error(f"Task execution error: {e}")
                span.set_status(trace.StatusCode.ERROR, str(e))
                span.record_exception(e)
                await redis_client.r.incr("stats:failed")
                
                if getattr(tasks, 'retry_count', 0) >= 3:
                    logger.error(f"[DLQ] Task {task_id} failed after 3 retries. Moving to DLQ.")
                    span.set_attribute("task.status", "DeadLetter")
                    async with redis_client.r.pipeline(transaction=True) as pipe:
                        pipe.lpush("dlq", task_json)
                        pipe.incr("stats:dlq")
                        pipe.hset(f"task:{task_id}", mapping={
                            "task_id": task_id,
                            "status": "DeadLetter",
                            "error": f"Failed after 3 retries. Last error: {str(e)}"
                        })
                        await pipe.execute()
                else:
                    tasks.retry_count += 1
                    BASE = 1.0
                    MAX_DELAY = 30.0
                    prev = getattr(tasks, 'prev_delay', BASE)
                    delay = min(MAX_DELAY, random.uniform(BASE, prev * 3))
                    tasks.prev_delay = delay

                    async with redis_client.r.pipeline(transaction=True) as pipe:
                        pipe.zadd("delayed_tasks", {json.dumps(tasks.model_dump()): time.time() + delay})
                        pipe.incr("stats:delayed")
                        pipe.hset(f"task:{task_id}", mapping={
                            "task_id": task_id,
                            "status": "RetryScheduled",
                            "retry_count": tasks.retry_count,
                            "error": f"Error: {str(e)}. Scheduled for retry in {delay:.2f}s."
                        })
                        await pipe.execute()
            
    finally:
        try:
            async with redis_client.r.pipeline(transaction=True) as pipe:
                pipe.lrem(f"processing_queue:{WORKER_ID}", count=1, value=task_json)
                if incr_done:
                    pipe.decr("stats:processing")
                pipe.publish("events:global", "update")
                await pipe.execute()
        except Exception as e:
            logger.error(f"[Cleanup] Redis error: {e}")
        finally:
            sem.release()

async def consumer_task():
    logger.info(
        f"Detected {_TOTAL_CORES} CPU cores. "
        f"Reserved={_RESERVED_CORES}, CPU workers={CPU_WORKERS}, "
        f"IO threads={IO_WORKERS}, Semaphore={CONCURRENCY}"
    )
    process_pool = ProcessPoolExecutor(max_workers=CPU_WORKERS)
    thread_pool = ThreadPoolExecutor(max_workers=IO_WORKERS)
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(CONCURRENCY)

    def _signal_handler():
        logger.info("Shutdown signal received. Cleaning up...")
        shutdown_event.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:
        signal.signal(signal.SIGINT, lambda s, f: _signal_handler())
        signal.signal(signal.SIGTERM, lambda s, f: _signal_handler())
    
    while not shutdown_event.is_set():
        try:
            while True:
                leftover = await redis_client.r.rpoplpush(f"processing_queue:{WORKER_ID}", "queue:high")
                if not leftover:
                    break
                await redis_client.r.incr("stats:pending")
                await redis_client.r.decr("stats:processing")
                logger.info(f"Recovered crashed task on startup: {leftover}")
            break
        except Exception as e:
            logger.error(f"[Startup] Redis unavailable ({e.__class__.__name__}). Retrying in 5s...")
            await asyncio.sleep(5)

    asyncio.create_task(cron_scheduler())
    asyncio.create_task(retry_scheduler())
    asyncio.create_task(heartbeat())
    asyncio.create_task(zombie_sweeper())
    
    logger.info(f"Worker {WORKER_ID} started and ready to process tasks.")
    
    while not shutdown_event.is_set():
        try:
            await sem.acquire()
            task_json = await redis_client.r.rpoplpush("queue:high", f"processing_queue:{WORKER_ID}")
            if not task_json:
                task_json = await redis_client.r.rpoplpush("queue:default", f"processing_queue:{WORKER_ID}")
            if not task_json:
                task_json = await redis_client.r.rpoplpush("queue:low", f"processing_queue:{WORKER_ID}")
            
            if not task_json:
                await asyncio.sleep(1)
                sem.release()
                continue
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
        
    thread_pool.shutdown(wait=True)
    process_pool.shutdown(wait=True)
    await redis_client.r.aclose()
    
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()

if __name__ == "__main__":
    asyncio.run(consumer_task())
