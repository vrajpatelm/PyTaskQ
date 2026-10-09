<div align="center">
  <h1>🚀 PyTaskQ</h1>
  <p><b>A highly scalable, asynchronous distributed task queue for Python.</b></p>
  <p>Inspired by Celery, but built natively for the modern async ecosystem (asyncio, FastAPI, Redis).</p>
</div>

---

## ⚡ What is PyTaskQ?

PyTaskQ is an enterprise-grade background task processing framework. It allows you to offload heavy CPU or I/O bound operations from your main application thread to a fleet of distributed workers, ensuring your application remains incredibly fast and responsive.

### 🌟 Key Architectural Features

- **Celery-Like SDK:** Beautiful, intuitive Python decorators (`@queue.task()`) that grant you `.delay()` and `.schedule()` capabilities instantly.
- **Hybrid Concurrency Engine:** Automatically routes tasks to a `ProcessPoolExecutor` (for heavy CPU bounds) or a `ThreadPoolExecutor` (for I/O bound tasks) to maximize resource utilization.
- **Atomic Execution & Fencing:** Utilizes Redis Lua scripting to implement strict fencing tokens. This guarantees that if a worker hangs or zombies, its late results won't overwrite a successfully re-assigned task.
- **Distributed Cron (Beat):** Built-in distributed scheduling with Leader Election. Multiple worker nodes can run concurrently without race conditions, safely scheduling recurring tasks via atomic locks.
- **Crash Recovery & Zombie Sweepers:** Workers constantly monitor the fleet via Redis heartbeats. If a worker is OOM-killed or loses power mid-execution, a zombie sweeper automatically recovers its in-flight tasks and re-queues them.
- **Resilient Retries:** Failed tasks undergo exponential backoff with decorrelated jitter, before being safely parked in a Dead Letter Queue (DLQ).
- **Strict Priority Queues:** Three-tier routing (`high`, `default`, `low`) ensures critical tasks bypass the bulk processing backlog.

---

## 🛠️ Quick Start

**Prerequisites**: Python 3.10+ and a running instance of Redis.

### 1. Define your tasks

Simply import the PyTaskQ registry and decorate your functions:

```python
# main.py
from pytaskq.task_registery import queue
import time

@queue.task(type="cpu")
def heavy_computation(matrix_size):
    # Heavy CPU work runs safely in a Process Pool!
    return matrix_size * matrix_size

@queue.task(type="io", cron="0 0 * * *") # Runs daily at midnight
def daily_db_cleanup():
    # I/O bound work runs in a Thread Pool!
    pass
```

### 2. Trigger them effortlessly

Anywhere in your application (like a FastAPI route), just call `.delay()`:

```python
# Trigger immediately (async non-blocking)
await heavy_computation.delay(500, priority="high")

# Or schedule it to run 1 hour from now
await heavy_computation.schedule(500, delay_seconds=3600)
```

### 3. Start the Worker Fleet

Spin up as many workers as you need across different machines:

```bash
python src/worker.py
```

---

## 🏗️ Deep Dive into the Architecture

PyTaskQ was built to handle failure gracefully. Here is how we guarantee task safety:

1. **The Processing Queue:** When a worker pops a task from `queue:high`, it atomically moves it to `processing_queue:{worker_id}`. If the machine catches fire, the task is safely preserved in Redis.
2. **The Zombie Sweeper:** The fleet elects a "Leader" worker. The leader monitors the `active_workers` sorted set. If a worker misses its heartbeat by 30 seconds, the leader sweeps its `processing_queue` and returns those tasks to the main queue.
3. **Fencing Tokens:** To prevent the "split-brain" problem (where a zombie worker wakes up 10 minutes later and overwrites the retry's result), every task payload contains a `fence_token`. The execution Lua script enforces that only the highest token is allowed to write the final state.

---

## 📊 Real-Time Dashboard & API

PyTaskQ ships with a built-in REST API and React Dashboard for observing your fleet.

| Method | Route | Description |
|--------|-------|-------------|
| `GET` | `/task/{task_id}` | Check task status and fetch the result |
| `GET` | `/metrics` | View queue depth across all priority levels |
| `GET` | `/dlq` | View failed tasks sitting in the Dead Letter Queue |
| `POST` | `/dlq/retry_all` | Atomically sweep all failed tasks back to the active queue |

---

## 🧪 Testing

The test suite relies heavily on `pytest` and `fakeredis` with Lua scripting support, meaning you can test distributed race conditions entirely in-memory!

```bash
pip install -r requirements.txt
python -m pytest tests/
```

*Note: The suite aggressively tests SSRF guards, idempotent retry lifecycles, and crash recovery loops.*
