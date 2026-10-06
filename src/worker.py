from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from src.schemas.requests import Taskresult,Taskloader
import json
import time
from typing import Any,List
import uuid
from src.task_registery import TASKS
import asyncio
import redis.asyncio as redis
import signal
import os
import urllib.request
import logging
import random
from opentelemetry import trace
from src.tracing import init_tracer, extract_trace_context, inject_trace_context

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("worker")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WORKER_ID=str(uuid.uuid4())

# Hard wall-clock limit for any single task execution. Without it, one hung
# handler (no socket timeout, infinite loop) holds a semaphore slot forever
# and 10 wedged tasks deadlock the whole worker. 0 disables the limit.
TASK_TIMEOUT = float(os.getenv("TASK_TIMEOUT", "300"))  # seconds (default: 5 min)


# Override via environment variable in docker-compose.yml
_TOTAL_CORES   = os.cpu_count() or 1
_RESERVED_CORES = int(os.getenv("WORKER_RESERVED_CORES", "1"))
CPU_WORKERS    = max(1, _TOTAL_CORES - _RESERVED_CORES)
IO_WORKERS     = max(4, _TOTAL_CORES * 4)   
CONCURRENCY    = CPU_WORKERS + IO_WORKERS    # Total semaphore slots

class TaskTimeoutError(Exception):
    """Raised when a task exceeds TASK_TIMEOUT. Treated as a retryable failure."""
    
from src.core import redis_client
shutdown_event = asyncio.Event()
active_tasks = set()

# OpenTelemetry: Initialize tracer for worker spans 

tracer = init_tracer("pytaskq-worker")

#Fencing Token 
FENCE_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    redis.call('hset', KEYS[2], 'task_id', ARGV[2], 'status', ARGV[3], 'result', ARGV[4], 'tenant_id', ARGV[5])
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

# ── Leader Election ────────────────────────────────────────────────────────── 
# If the leader crashes, the lock expires in 20s and another worker takes over.

async def is_leader() -> bool:
    try:
        # 1. Try to acquire the lock (only succeeds if lock doesn't exist)
        acquired = await redis_client.r.set("pytaskq:leader_lock", WORKER_ID, nx=True, ex=20)
        if acquired:
            return True
            
        # 2. If we didn't acquire it, check if we ALREADY own it (renew it)
        current_leader = await redis_client.r.get("pytaskq:leader_lock")
        if current_leader == WORKER_ID:
            await redis_client.r.expire("pytaskq:leader_lock", 20)
            return True
            
        return False
    except Exception as e:
        logger.warning(f"[Leader Election] Redis error: {e}")
        return False

#Background scheduler that moves delayed tasks back into task_queue 
# when their time comes

async def retry_scheduler():
    while True:
        if not await is_leader():
            await asyncio.sleep(5)
            continue
            
        now = time.time()
        ready_tasks = await redis_client.r.zrangebyscore("delayed_tasks", "-inf", now)
        for i, task_json in enumerate(ready_tasks):
            # If we are processing a massive backlog, renew the lock every 100 tasks
            if i > 0 and i % 100 == 0:
                await redis_client.r.expire("pytaskq:leader_lock", 20)
                
            try:
                task_data = json.loads(task_json)
                tenant_id = task_data.get('tenant_id', 'unknown')
                priority = task_data.get('priority', 'default')
                
                requeued = await redis_client.r.eval(
                    REQUEUE_SCRIPT,
                    4,
                    "delayed_tasks",
                    f"queue:{priority}",
                    f"stats:delayed:{tenant_id}",
                    f"stats:pending:{tenant_id}",
                    task_json
                )
                
                if requeued:
                    logger.info(f"[Retry Scheduler] Re-queued task {task_data.get('task_id')} "
                          f"(retry #{task_data.get('retry_count')})")
            except Exception as e:
                logger.error(f"[Retry Scheduler] Error processing task: {e}")
        
        await asyncio.sleep(1)  

async def heartbeat():
    while True:
        try:
            await redis_client.r.zadd("active_workers",{WORKER_ID:time.time()+30})
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
                    # Read the last element without removing it (safe because worker is dead)
                    tasks = await redis_client.r.lrange(f"processing_queue:{worker_id}", -1, -1)
                    if not tasks:
                        break  # No more tasks for this dead worker
                    
                    task = tasks[0]
                    # If this dead worker had a massive backlog, renew lock every 100 tasks
                    if tasks_recovered > 0 and tasks_recovered % 100 == 0:
                        await redis_client.r.expire("pytaskq:leader_lock", 20)
                        
                    task_data = json.loads(task)
                    task_data["fence_token"] += 1
                    task_id = task_data.get("task_id")
                    # Part B: write the NEW token to Redis so old workers know they are stale
                    tenant_id = task_data.get('tenant_id', 'unknown')
                    
                    # Atomic recovery: remove original task string and push updated one
                    async with redis_client.r.pipeline(transaction=True) as pipe:
                        pipe.lrem(f"processing_queue:{worker_id}", count=1, value=task)
                        pipe.set(f"fence:{task_id}", task_data["fence_token"], ex=86400)
                        pipe.lpush("queue:high", json.dumps(task_data))
                        pipe.incr(f"stats:pending:{tenant_id}")
                        pipe.decr(f"stats:processing:{tenant_id}")
                        await pipe.execute()
                    
                    tasks_recovered += 1
                    
                # NOW remove the dead worker from the registry
                await redis_client.r.zrem("active_workers", worker_id)
                logger.info(f"[Sweeper] Recovered tasks from dead worker: {worker_id}")
        except Exception as e:
            logger.error(f"[Sweeper] Redis error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        await asyncio.sleep(15)

async def consumer_task():
    # Dynamically computed at startup based on os.cpu_count() and WORKER_RESERVED_CORES env var.
    # Shared box (web+worker together): CPU_WORKERS = max(1, cores - 1)
    # Dedicated worker machine:          CPU_WORKERS = max(1, cores - 0) = all cores
    logger.info(
        f"[Pool] Detected {_TOTAL_CORES} CPU cores. "
        f"Reserved={_RESERVED_CORES}, CPU workers={CPU_WORKERS}, "
        f"IO threads={IO_WORKERS}, Semaphore={CONCURRENCY}"
    )
    process_pool = ProcessPoolExecutor(max_workers=CPU_WORKERS)
    thread_pool  = ThreadPoolExecutor(max_workers=IO_WORKERS)
    loop = asyncio.get_running_loop()
    sem  = asyncio.Semaphore(CONCURRENCY)
    # for tasks that were being processed when the worker crashed, move them back to the main queue for reprocessing
    def _signal_handler():
        logger.info("Shutdown signal received. Cleaning up...")
        shutdown_event.set()
    # We use a try/except so the worker runs on both platforms.
    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:
        # Windows fallback: use the standard signal module instead
        signal.signal(signal.SIGINT,  lambda s, f: _signal_handler())
        signal.signal(signal.SIGTERM, lambda s, f: _signal_handler())
    
    # ── Startup: crash recovery with Redis retry ─────────────────────────────
    # If Redis is down when the worker starts, we wait and retry instead of
    # crashing immediately. This makes the worker resilient to Redis restarts.
    while not shutdown_event.is_set():
        try:
            while True:
                leftover = await redis_client.r.rpoplpush(f"processing_queue:{WORKER_ID}", "queue:high")
                if not leftover:
                    break
                task_data = json.loads(leftover)
                tenant_id = task_data.get('tenant_id', 'unknown')
                await redis_client.r.incr(f"stats:pending:{tenant_id}")
                await redis_client.r.decr(f"stats:processing:{tenant_id}")
                logger.info(f"Recovered crashed task on startup: {leftover}")
            break  # Recovery succeeded — exit the retry loop and continue
        except Exception as e:
            logger.error(f"[Startup] Redis unavailable ({e.__class__.__name__}). "
                  f"Retrying in 5s...")
            await asyncio.sleep(5)

    asyncio.create_task(retry_scheduler())
    asyncio.create_task(heartbeat())
    asyncio.create_task(zombie_sweeper())
    
    logger.info(f"Worker {WORKER_ID} started and ready to process tasks.")
    
    while not shutdown_event.is_set():

        # Unpack the Redis response first
        try:
            await sem.acquire()  # Wait for a free slot
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
            sem.release()  # ← CRITICAL: release slot even on connection error
            logger.error(f"[Main loop] Redis error: {e.__class__.__name__}. Backing off 5s...")
            await asyncio.sleep(5)
            continue


        
        """
        task: It holds the refrence of The task .
        """
        
        task = asyncio.create_task(
            handle_task(task_json, sem, loop, process_pool, thread_pool)
        )
        active_tasks.add(task)
        task.add_done_callback(active_tasks.discard)
    if active_tasks:
        logger.info(f"Waiting for active tasks of no {len(active_tasks)} to complete...")
        await asyncio.gather(*active_tasks, return_exceptions=True )
    thread_pool.shutdown(wait=True)
    process_pool.shutdown(wait=True)
    await redis_client.r.aclose()
    
    # ── OpenTelemetry: Flush remaining spans before exiting ────────────
    # Ensures no traces are lost if the worker is killed/restarted.
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()


async def handle_task(task_json, sem, loop, process_pool, thread_pool):
    incr_done = False  # Guard: only DECR stats:processing if we actually INCRed it
    try:  
        # 1. Validation
        try:
            tasks = Taskloader.model_validate_json(task_json)
            task_id = tasks.task_id
            my_token = tasks.fence_token  # remember the token we were given
            tenant_id = getattr(tasks, 'tenant_id', 'unknown')
            await redis_client.r.decr(f"stats:pending:{tenant_id}")

        except Exception as e:
            logger.error(f"Error occurred while validating task JSON: {e}")
            task_id = "Unknown"
            await redis_client.r.hset(f"task:{task_id}", mapping={
                "task_id": task_id,
                "status": "Failed",
                "error": f"JSON Validation Error: {str(e)}"
            })
            return  

        # 2. Execution
        # ── OpenTelemetry: Extract parent trace from task payload ─────
        parent_ctx = extract_trace_context(getattr(tasks, 'trace_carrier', {}))
        with tracer.start_as_current_span(
            "execute_task",
            context=parent_ctx,
            attributes={
                "task.id": task_id,
                "task.name": tasks.task_name,
                "task.retry_count": tasks.retry_count,
                "worker.id": WORKER_ID,
                "tenant.id": tenant_id,
            },
        ) as span:
            try:
                entry = TASKS[tasks.task_name]   # KeyError caught below if unknown
                func = entry["handler"]
                task_type = entry["type"]
                
                logger.info(f"Executing task {task_id} ({tasks.task_name})")
                await redis_client.r.incr(f"stats:processing:{tenant_id}")  # Atomic counter: task is now actively executing
                incr_done = True  # Mark that we INCRed so finally block will DECR

                if task_type == "cpu":
                    # Run in a process to use another CPU core without GIL blocking
                    exec_future = loop.run_in_executor(process_pool, func, *tasks.args)
                else:
                    # Run in a thread for low-overhead I/O tasks
                    exec_future = loop.run_in_executor(thread_pool, func, *tasks.args)

                if TASK_TIMEOUT > 0:
                    try:
                        result = await asyncio.wait_for(exec_future, timeout=TASK_TIMEOUT)
                    except asyncio.TimeoutError:
                        # The executor worker is STILL RUNNING the handler — wait_for
                        # cannot kill a thread/process. Invalidate our fence token so
                        # whenever the orphaned handler eventually finishes, its result
                        # write fails the fence check and is discarded.
                        await redis_client.r.incr(f"fence:{task_id}")
                        # Bound this key's lifetime like the sweeper does —
                        # without a TTL one fence key leaks per timed-out task.
                        await redis_client.r.expire(f"fence:{task_id}", 86400)
                        raise TaskTimeoutError(
                            f"Task exceeded {TASK_TIMEOUT}s limit (execution abandoned; "
                            f"late result will be discarded by fence check)"
                        ) from None
                else:
                    result = await exec_future  # no timeout configured

                
            # Save Success result to Redis — atomically via Lua fencing check
                saved = await redis_client.r.eval(
                    FENCE_SCRIPT,
                    2,                        # number of KEYS passed
                    f"fence:{task_id}",       # KEYS[1] — the token we check against
                    f"task:{task_id}",        # KEYS[2] — the hash we write result into
                    str(my_token),            # ARGV[1] — our claimed token
                    task_id,                  # ARGV[2] — hset field: task_id
                    "Success",               # ARGV[3] — hset field: status
                    str(result),              # ARGV[4] — hset field: result
                    tenant_id                 # ARGV[5] — hset field: tenant_id (isolation)
                )
                if saved:
                    async with redis_client.r.pipeline(transaction=True) as pipe:
                        pipe.expire(f"task:{task_id}", 86400)
                        pipe.incr(f"stats:completed:{tenant_id}")
                        await pipe.execute()
                    span.set_attribute("task.status", "Success")
                    logger.info(f"Task {task_id} completed and saved ")
                else:
                    span.set_attribute("task.status", "Discarded")
                    span.set_attribute("task.discard_reason", "fence_token_stale")
                    logger.warning(
                        f"Task {task_id} result DISCARDED — fence token stale "
                        f"(my_token={my_token}). Task was re-assigned to another worker."
                    )
            
            # --- WEBHOOK FEATURE ---
            # Only fire webhook if our result was actually saved (not discarded by fencing check)
                if saved and getattr(tasks, 'webhook_url', None) and tasks.task_name != "_deliver_webhook":
                    with tracer.start_as_current_span(
                        "enqueue_webhook_delivery",
                        attributes={"task.id": task_id, "webhook.url": tasks.webhook_url},
                    ):
                        payload = {"task_id": task_id, "status": "Success", "result": str(result)}
                        webhook_secret = await redis_client.r.hget(f"webhook:{tenant_id}", "secret")
                        
                        if webhook_secret:
                            webhook_task_id = str(uuid.uuid4())
                            webhook_task_json = json.dumps({
                                "task_name": "_deliver_webhook",
                                "args": [tasks.webhook_url, payload, webhook_secret],
                                "task_id": webhook_task_id,
                                "retry_count": 0,
                                "webhook_url": None, # don't webhook a webhook!
                                "priority": "high",
                                "tenant_id": tenant_id,
                                "client_ip": getattr(tasks, 'client_ip', 'unknown'),
                                "trace_carrier": inject_trace_context(),
                            })
                            # Atomic enqueue
                            async with redis_client.r.pipeline(transaction=True) as pipe:
                                pipe.lpush("queue:high", webhook_task_json)
                                pipe.incr(f"stats:pending:{tenant_id}")
                                await pipe.execute()
                            logger.info(f"Enqueued _deliver_webhook task {webhook_task_id} for original task {task_id}")
                        else:
                            logger.warning(f"Task {task_id} has webhook_url but no secret found for tenant {tenant_id}")
                
            except Exception as e:
                logger.error(f"Error occurred while executing task: {e}")
                # Record the error on the OTel span so it shows as a red error in Jaeger
                span.set_status(trace.StatusCode.ERROR, str(e))
                span.record_exception(e)
                await redis_client.r.incr(f"stats:failed:{tenant_id}")   # Cumulative: total tasks ever failed/retried
                if getattr(tasks, 'retry_count', 0) >= 3:
                   logger.error(f"[DLQ] Task {task_id} failed after 3 retries. Moving to dead-letter queue.")
                   span.set_attribute("task.status", "DeadLetter")
                   
                   # Atomic move to DLQ
                   async with redis_client.r.pipeline(transaction=True) as pipe:
                       pipe.lpush(f"dlq:{tenant_id}", task_json)
                       pipe.incr(f"stats:dlq:{tenant_id}")
                       pipe.hset(f"task:{task_id}", mapping={
                            "task_id": task_id,
                            "status": "DeadLetter",
                            "tenant_id": tenant_id,
                            "error": f"Failed after 3 retries.Last error: {str(e)} "}
                        )
                       await pipe.execute()
                else:
                    tasks.retry_count += 1

                    # Decorrelated Jitter
                    BASE      = 1.0
                    MAX_DELAY = 30.0
                    prev      = getattr(tasks, 'prev_delay', BASE)
                    delay     = min(MAX_DELAY, random.uniform(BASE, prev * 3))
                    tasks.prev_delay = delay   # save so NEXT retry can use it

                    # Atomic scheduling
                    async with redis_client.r.pipeline(transaction=True) as pipe:
                        pipe.zadd("delayed_tasks", {json.dumps(tasks.model_dump()): time.time() + delay})
                        pipe.incr(f"stats:delayed:{tenant_id}")
                        pipe.hset(f"task:{task_id}", mapping={
                            "task_id": task_id,
                            "status": "RetryScheduled",
                            "tenant_id": tenant_id,
                            "retry_count": tasks.retry_count,
                            "error": f"Error: {str(e)}. Scheduled for retry in {delay:.2f} seconds."
                        })
                        await pipe.execute()
            
    finally:
        try:
            # Atomic cleanup
            async with redis_client.r.pipeline(transaction=True) as pipe:
                pipe.lrem(f"processing_queue:{WORKER_ID}", count=1, value=task_json)
                if incr_done:  # Only DECR if we actually INCRed
                    pipe.decr(f"stats:processing:{tenant_id}")
                await pipe.execute()
        except Exception as e:
            logger.error(f"[Cleanup] Redis error during cleanup: {e}")
        finally:
            sem.release()

#Start the event loop
if __name__ == "__main__":
    asyncio.run(consumer_task())
