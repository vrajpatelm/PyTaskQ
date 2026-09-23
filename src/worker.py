from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from src.Schema import Taskresult,Taskloader
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
    
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
shutdown_event = asyncio.Event()
active_tasks = set()

#Background scheduler that moves delayed tasks back into task_queue 
# when their time comes

async def retry_scheduler():
    while True:
        now = time.time()
        ready_tasks = await r.zrangebyscore("delayed_tasks", "-inf", now)
        for task_json in ready_tasks:
            removed = await r.zrem("delayed_tasks", task_json)
            if removed:  
                task_data = json.loads(task_json)
                await r.lpush(f"queue:{task_data.get('priority', 'default')}", task_json)
                tenant_id = task_data.get('tenant_id', 'unknown')
                await r.decr(f"stats:delayed:{tenant_id}")
                await r.incr(f"stats:pending:{tenant_id}")
                logger.info(f"[Retry Scheduler] Re-queued task {task_data.get('task_id')} "
                      f"(retry #{task_data.get('retry_count')})")
        
        await asyncio.sleep(1)  

async def heartbeat():
    while True:
        try:
            await r.zadd("active_workers",{WORKER_ID:time.time()+30})
            await asyncio.sleep(10)
        except Exception as e:
            logger.warning(f"[Heartbeat] Redis unreachable: {e}. Retrying in 5s")
            await asyncio.sleep(5)
    
async def zombie_sweeper():
    while True:
        try:
            expired = await r.zrangebyscore("active_workers", "-inf", time.time())
            for worker_id in expired:
                while True:
                    result = await r.rpoplpush(f"processing_queue:{worker_id}", f"queue:high")
                    if not result:
                        break  # No more tasks for this dead worker
                    task_data = json.loads(result)
                    tenant_id = task_data.get('tenant_id', 'unknown')
                    await r.incr(f"stats:pending:{tenant_id}")
                    await r.decr(f"stats:processing:{tenant_id}")
                # NOW remove the dead worker from the registry
                await r.zrem("active_workers", worker_id)
                logger.info(f"[Sweeper] Recovered tasks from dead worker: {worker_id}")
        except Exception as e:
            logger.error(f"[Sweeper] Redis error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        await asyncio.sleep(15)

async def consumer_task():
    process_pool = ProcessPoolExecutor(max_workers=4)
    thread_pool = ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(10)  # Limit concurrent tasks to 10
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
                leftover = await r.rpoplpush(f"processing_queue:{WORKER_ID}", "queue:high")
                if not leftover:
                    break
                task_data = json.loads(leftover)
                tenant_id = task_data.get('tenant_id', 'unknown')
                await r.incr(f"stats:pending:{tenant_id}")
                await r.decr(f"stats:processing:{tenant_id}")
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
            task_json = await r.rpoplpush("queue:high", f"processing_queue:{WORKER_ID}")
            if not task_json:
                task_json = await r.rpoplpush("queue:default", f"processing_queue:{WORKER_ID}")
            if not task_json:
               task_json = await r.rpoplpush("queue:low", f"processing_queue:{WORKER_ID}")
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
    await r.aclose()


async def handle_task(task_json, sem, loop, process_pool, thread_pool):
    incr_done = False  # Guard: only DECR stats:processing if we actually INCRed it
    try:  # <--- Outer try block starts here
        # 1. Validation
        try:
            tasks = Taskloader.model_validate_json(task_json)
            task_id = tasks.task_id
            tenant_id = getattr(tasks, 'tenant_id', 'unknown')
            await r.decr(f"stats:pending:{tenant_id}")

        except Exception as e:
            logger.error(f"Error occurred while validating task JSON: {e}")
            task_id = "Unknown"
            await r.hset(f"task:{task_id}", mapping={
                "task_id": task_id,
                "status": "Failed",
                "error": f"JSON Validation Error: {str(e)}"
            })
            return  

        # 2. Execution
        # NOTE: TASKS lookup is INSIDE the try block.
        # If task_name is unknown, KeyError is caught here
        # and goes through the normal retry → DLQ pipeline.
        try:
            entry = TASKS[tasks.task_name]   # KeyError caught below if unknown
            func = entry["handler"]
            task_type = entry["type"]
            
            logger.info(f"Executing task {task_id} ({tasks.task_name})")
            await r.incr(f"stats:processing:{tenant_id}")  # Atomic counter: task is now actively executing
            incr_done = True  # Mark that we INCRed so finally block will DECR

            if task_type == "cpu":
                # Run in a process to use another CPU core without GIL blocking
                result = await loop.run_in_executor(process_pool, func, *tasks.args)
            else:
                # Run in a thread for low-overhead I/O tasks
                result = await loop.run_in_executor(thread_pool, func, *tasks.args)

                
            # Save Success result to Redis
            task_id = tasks.task_id
            task_result = Taskresult(task_id=task_id, status="Success", result=str(result))
            await r.hset(f"task:{task_id}", mapping=task_result.model_dump())
            await r.expire(f"task:{task_id}", 86400)
            await r.incr(f"stats:completed:{tenant_id}")  # Cumulative: total tasks ever completed
            logger.info(f"Task {task_id} completed successfully.")
            
            # --- WEBHOOK FEATURE ---
            if getattr(tasks, 'webhook_url', None) and tasks.task_name != "_deliver_webhook":
                payload = {"task_id": task_id, "status": "Success", "result": str(result)}
                webhook_secret = await r.hget(f"webhook:{tenant_id}", "secret")
                
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
                        "client_ip": getattr(tasks, 'client_ip', 'unknown')
                    })
                    await r.lpush("queue:high", webhook_task_json)
                    await r.incr(f"stats:pending:{tenant_id}")
                    logger.info(f"Enqueued _deliver_webhook task {webhook_task_id} for original task {task_id}")
                else:
                    logger.warning(f"Task {task_id} has webhook_url but no secret found for tenant {tenant_id}")
            
        except Exception as e:
            logger.error(f"Error occurred while executing task: {e}")
            await r.incr(f"stats:failed:{tenant_id}")   # Cumulative: total tasks ever failed/retried
            if getattr(tasks, 'retry_count', 0) >= 3:
               logger.error(f"[DLQ] Task {task_id} failed after 3 retries. Moving to dead-letter queue.")
               await r.lpush(f"dlq:{tenant_id}", task_json)
               await r.incr(f"stats:dlq:{tenant_id}")
               await r.hset(f"task:{task_id}", mapping={
                    "task_id": task_id,
                    "status": "DeadLetter",
                    "error": f"Failed after 3 retries.Last error: {str(e)} "}
                )
            else:
                tasks.retry_count += 1
                delay = 2 ** tasks.retry_count  # Exponential backoff
                await r.zadd("delayed_tasks", {json.dumps(tasks.model_dump()): time.time() + delay})
                await r.incr(f"stats:delayed:{tenant_id}")
                logger.warning(f"[Retry] Task {task_id} failed. Scheduled for retry #{tasks.retry_count} after {delay} seconds.")
                await r.hset(f"task:{task_id}", mapping={
                    "task_id": task_id,
                    "status": "RetryScheduled",
                    "retry_count": tasks.retry_count,
                    "error": f"Error: {str(e)}. Scheduled for retry in {delay} seconds."
                })
            
    finally:
        await r.lrem(f"processing_queue:{WORKER_ID}", count=1, value=task_json)
        if incr_done:  # Only DECR if we actually INCRed — prevents counter going negative
            await r.decr(f"stats:processing:{tenant_id}")
        sem.release()

#Start the event loop
if __name__ == "__main__":
    asyncio.run(consumer_task())