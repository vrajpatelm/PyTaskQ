from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
import time
from typing import Any,List
import uuid
from pydantic import BaseModel
from src.task_registery import TASKS
import asyncio
import redis.asyncio as redis
import signal
import os


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
WORKER_ID=str(uuid.uuid4())

# for incoming taks VALIDATION
class Taskloader(BaseModel):
    task_id:str
    task_name:str
    args:List[Any]
    retry_count:int = 0 
    task_type:str = "io"  # Legacy field — execution type is now looked up from the registry
# for VALIDATION of result of task 
class Taskresult(BaseModel):
    task_id:str
    status:str
    result:str
    
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
                await r.lpush("task_queue", task_json)
                task_data = json.loads(task_json)
                print(f"[Retry Scheduler] Re-queued task {task_data.get('task_id')} "
                      f"(retry #{task_data.get('retry_count')})")
        
        await asyncio.sleep(1)  

async def heartbeat():
    while True:
        try:
            await r.zadd("active_workers",{WORKER_ID:time.time()+30})
            await asyncio.sleep(10)
        except Exception as e:
            print(f"[Heartbeat] Redis unreachable: {e}. Retrying in 5s")
            await asyncio.sleep(5)
    
async def zombie_sweeper():
    while True:
        try:
            expired = await r.zrangebyscore("active_workers", "-inf", time.time())
            for worker_id in expired:
                while True:
                    result = await r.rpoplpush(f"processing_queue:{worker_id}", "task_queue")
                    if not result:
                        break  # No more tasks for this dead worker
                # NOW remove the dead worker from the registry
                await r.zrem("active_workers", worker_id)
                print(f"[Sweeper] Recovered tasks from dead worker: {worker_id}")
        except Exception as e:
            print(f"[Sweeper] Redis error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        await asyncio.sleep(15)

async def consumer_task():
    process_pool = ProcessPoolExecutor(max_workers=4)
    thread_pool = ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(10)  # Limit concurrent tasks to 10
    # for tasks that were being processed when the worker crashed, move them back to the main queue for reprocessing
    def _signal_handler():
        print("Shutdown signal received. Cleaning up...")
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
                leftover = await r.rpoplpush(f"processing_queue:{WORKER_ID}", "task_queue")
                if not leftover:
                    break
                print(f"Recovered crashed task on startup: {leftover}")
            break  # Recovery succeeded — exit the retry loop and continue
        except Exception as e:
            print(f"[Startup] Redis unavailable ({e.__class__.__name__}). "
                  f"Retrying in 5s...")
            await asyncio.sleep(5)

    asyncio.create_task(retry_scheduler())
    asyncio.create_task(heartbeat())
    asyncio.create_task(zombie_sweeper())
    while not shutdown_event.is_set():

        # Unpack the Redis response first
        try:
            await sem.acquire()  # Wait for a free slot
            task_json = await r.brpoplpush("task_queue", f"processing_queue:{WORKER_ID}", timeout=2)
            if task_json is None:
                sem.release()  # Release the semaphore if no task was fetched
                continue
        except Exception as e:
            sem.release()  # ← CRITICAL: release slot even on connection error
            print(f"[Main loop] Redis error: {e.__class__.__name__}. Backing off 5s...")
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
        print(f"Waiting for active tasks of no {len(active_tasks)} to complete...")
        await asyncio.gather(*active_tasks, return_exceptions=True )
    thread_pool.shutdown(wait=True)
    process_pool.shutdown(wait=True)
    await r.aclose()
        
async def handle_task(task_json, sem, loop, process_pool, thread_pool):
    try:  # <--- Outer try block starts here
        # 1. Validation
        try:
            tasks = Taskloader.model_validate_json(task_json)
            task_id = tasks.task_id
        except Exception as e:
            print(f"Error occurred while validating task JSON: {e}")
            task_id = "Unknown"
            await r.hset(f"Task id{task_id}", mapping={
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
            if task_type == "cpu":
                # Run in a process to use another CPU core without GIL blocking
                result = await loop.run_in_executor(process_pool, func, *tasks.args)
            else:
                # Run in a thread for low-overhead I/O tasks
                result = await loop.run_in_executor(thread_pool, func, *tasks.args)

                
            # Save Success result to Redis
            task_id = tasks.task_id
            task_result = Taskresult(task_id=task_id, status="Success", result=str(result))
            await r.hset(f"Task id{task_id}", mapping=task_result.model_dump())
            await r.expire(f"Task id{task_id}", 86400)
            
        except Exception as e:
            print(f"Error occurred while executing task: {e}")
            if tasks.retry_count >= 3:
               print(f"[DLQ] Task {task_id} failed after 3 retries. Moving to dead-letter queue.")
               await r.lpush("dead_letter_queue", task_json)
               await r.hset(f"Task id{tasks.task_id}", mapping={
                    "task_id": tasks.task_id,
                    "status": "DeadLetter",
                    "error": f"Failed after 3 retries.Last error: {str(e)} "}
                )
            else:
                tasks.retry_count += 1
                delay = 2 ** tasks.retry_count  # Exponential backoff
                await r.zadd("delayed_tasks", {json.dumps(tasks.model_dump()): time.time() + delay})
                print(f"[Retry] Task {task_id} failed. Scheduled for retry #{tasks.retry_count} after {delay} seconds.")
                await r.hset(f"Task id{tasks.task_id}", mapping={
                    "task_id": tasks.task_id,
                    "status": "RetryScheduled",
                    "retry_count": tasks.retry_count,
                    "error": f"Error: {str(e)}. Scheduled for retry in {delay} seconds."
                })
            
    finally:
        await r.lrem(f"processing_queue:{WORKER_ID}",count=1, value=task_json)
        sem.release()
#Start the event loop
if __name__ == "__main__":
    asyncio.run(consumer_task())