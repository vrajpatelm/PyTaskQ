# High-Level System Architecture

This document provides a comprehensive overview of the custom, asynchronous Redis-backed distributed task queuing system implemented in this workspace.

---

## 1. System Overview

The system is a lightweight, reliable distributed task queue built in Python. It is designed to decouple heavy calculations (CPU-bound) and external integrations (I/O-bound) from the client-facing HTTP interface.

The architecture comprises three primary physical layers:
1. **Client / Frontend Interface**: A single-page dashboard serving as the control panel.
2. **API Server (FastAPI)**: An asynchronous web service that accepts task submissions, reports status/metrics, and manages failed tasks.
3. **Worker Daemon (Asyncio)**: A background execution engine that pulls tasks, runs them concurrently, coordinates retries, and records execution outcomes.

These components coordinate via a central **Redis** instance which acts as the message broker, state cache, and persistence layer.

(docs/images/architecture.svg)

---

## 2. Key Components

### A. API Server ([src/app.py])
Built using **FastAPI**, this service runs asynchronously under `uvicorn` and handles the following:
* **Task Ingestion**: 
  * Accepts matrix multiplication requests via `GET /task/mul?size=N` (CPU task).
  * Accepts email requests via Form Data on `POST /task/send_email` (I/O task).
  * Generates a unique `task_id` (UUID v4) for each incoming request, envelopes the task metadata, and serializes it to JSON before pushing it to Redis.
* **Status Retrieval**: Exposes `GET /task/{task_id}` to fetch execution results or current state details.
* **Dead Letter Queue (DLQ) Management**: Provides endpoints to view (`GET /dlq`), replay (`POST /dlq/replay/{task_id}`), or delete (`POST /dlq/purge/{task_id}` & `POST /dlq/purge_all`) failed tasks.
* **Queue Metrics**: Exposes `GET /metrics` returning current sizes of all queues (pending, processing, delayed, dlq).
* **Fault Tolerance**: Utilizes an exception handler for `RedisConnectionError` to gracefully return a HTTP `503 Service Unavailable` status when Redis is down.

### B. Redis Broker & Storage
Redis serves multiple roles utilizing different data structures:
1. **Main Queue (`task_queue` - List)**: A FIFO queue storing pending serialized tasks.
2. **Processing Queue (`processing_queue` - List)**: Stores tasks currently being processed by workers. This acts as a backup for crash recovery.
3. **Delayed Tasks (`delayed_tasks` - Sorted Set / ZSET)**: Holds failed tasks waiting for retry schedules. The score is set to `current_epoch_time + backoff_delay_seconds`.
4. **Dead Letter Queue (`dead_letter_queue` - List)**: Stores tasks that have exceeded the maximum retry count of 3.
5. **Task Status Store (`Task id{task_id}` - Hash)**: Persistent storage for task execution state. Stores keys like `task_id`, `status` (`Success`, `RetryScheduled`, `DeadLetter`, `Failed`), `result` / `error`, and `retry_count`. Has a **24-hour expiration time (86,400s)**.

### C. Worker Daemon ([src/worker.py])
An independent Python script executing on an **asyncio event loop**. It manages:
* **Job Consumption**: Fetches jobs using the blocking atomic `BRPOPLPUSH` command to transition tasks from `task_queue` to `processing_queue` safely.
* **Concurrency Control**: Employs an `asyncio.Semaphore` capped at `10` concurrent tasks to limit system resource starvation.
* **Execution Dispatching**: Leverages execution pools depending on task specifications:
  * **CPU-bound tasks**: Dispatched to a `ProcessPoolExecutor` (configured with `max_workers=4`) to bypass the Python GIL and run tasks on separate CPU cores.
  * **I/O-bound tasks**: Dispatched to a `ThreadPoolExecutor` (configured with `max_workers=10`) to allow lightweight concurrent wait states without process overhead.
* **Retry Scheduler**: Runs a secondary async loop (`retry_scheduler`) that checks the `delayed_tasks` ZSET every second. Tasks whose scheduled time has passed are automatically moved back to `task_queue` for consumption.

### D. Task Registry ([src/task_registery.py])
A decoupled repository for actual task execution logic:
* Defines [matrix_multiply] (generating two random arrays and computing dot products) and [send_email] (using `yagmail` to perform SMTP mail dispatching).
* Registers them under a central `TASKS` dictionary, enabling dynamic lookup at runtime by the Worker Daemon.

---

## 3. Detailed Data Flow & Task Lifecycle

The lifecycle of a single task progresses through well-defined states:

```mermaid
stateDiagram-v2
    [*] --> Queued : API receives request & pushes task
    Queued --> Processing : Worker pulls task via BRPOPLPUSH
    
    state Processing {
        [*] --> Executing
        Executing --> RunSuccess : No exception
        Executing --> RunError : Exception raised
    }

    RunSuccess --> Success : Save result to Redis (expire 24h)
    Success --> [*]

    RunError --> RetryScheduled : retry_count < 3
    RunError --> DeadLetter : retry_count >= 3 (Final Failure)

    RetryScheduled --> Queued : Scheduler pulls from ZSET when ready
    DeadLetter --> Queued : User triggers replay via API (reset retry)
    DeadLetter --> Purged : User triggers purge via API
    Purged --> [*]
```

### Flow Walkthrough:
1. **Queueing**: A client triggers an API route. The API generates a UUID, creates a dictionary with `task_type` and initialized `retry_count=0`, and executes `LPUSH task_queue`.
2. **Reliable Fetch**: The Worker uses `BRPOPLPUSH task_queue processing_queue`. The item is popped from the main queue and instantly pushed to the processing queue. If the worker crashes immediately, the task remains inside the `processing_queue`.
3. **Execution**:
   * The worker loads the task JSON and validates it via a Pydantic model (`Taskloader`).
   * The task is looked up in the `TASKS` registry.
   * If `task_type == "cpu"`, it executes inside `ProcessPoolExecutor`.
   * If `task_type == "io"`, it executes inside `ThreadPoolExecutor`.
4. **Resolution / Failures**:
   * **On Success**: The worker stores the result in a Redis hash `Task id{task_id}` with state `"Success"`, sets a 24h TTL, deletes the task from `processing_queue` via `LREM`, and releases the semaphore slot.
   * **On Failure**: The exception is caught. 
     * If `retry_count` is less than 3, the worker increments `retry_count`, calculates exponential backoff delay (`2 ** retry_count`), and saves the task to `delayed_tasks` (ZSET) with the scheduled time. It sets the Redis hash state to `"RetryScheduled"`.
     * If `retry_count` is 3, the task is moved directly to `dead_letter_queue` (DLQ), and the Redis hash state is updated to `"DeadLetter"`.

---

## 4. Key Design Patterns & Resilience Features

### 1. Reliable Queueing (At-Least-Once Delivery)
By using `BRPOPLPUSH` instead of a simple pop, the system guarantees that if a worker crashes, the task is not lost. 
During worker startup, a **crash recovery routine** sweeps `processing_queue` and pushes any leftover tasks back to the `task_queue` to ensure reprocessing:

### 2. Resilient Redis Reconnections
If Redis is down on worker startup, the worker logs the error and backs off for 5 seconds rather than crashing immediately. In the main loop, if a Redis operation fails, the semaphore slot is freed, and the worker sleeps before resuming.

### 3. Dedicated CPU vs I/O Pools
Python's GIL (Global Interpreter Lock) restricts standard multi-threaded Python programs from running CPU-intensive operations concurrently. 
* By running CPU tasks (`matrix_multiply`) inside a **`ProcessPoolExecutor`**, separate OS processes are spawned, utilizing multiple CPU cores.
* By running I/O tasks (`send_email`) inside a **`ThreadPoolExecutor`**, the worker is able to handle network latency and wait states efficiently without spawning heavy OS processes.

### 4. Graceful Shutdown
The worker registers signal handlers for `SIGINT` (Ctrl+C) and `SIGTERM`. When received:
1. `shutdown_event` is set, stopping the main consumption loop.
2. The worker waits for currently executing tasks to complete using `asyncio.gather(*active_tasks)`.
3. The executor pools are shutdown cleanly, and the Redis connection is closed.
