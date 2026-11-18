"""
example_app.py — a small PyTaskQ app you can run as-is

    pytaskq worker    --app example_app:queue
    pytaskq dashboard --app example_app:queue      # http://localhost:8000
"""

import asyncio
import time

from pytaskq import PyTaskQ

# 1. Create the queue (app-wide defaults are optional)
queue = PyTaskQ("example", max_retries=3)


# 2. Define your tasks
@queue.task(type="cpu")
def matrix_multiply(size: int = 100):
    """Multiply two N x N matrices (CPU-bound, runs in a process pool)."""
    rows = [[float(i * size + j) for j in range(size)] for i in range(size)]
    return {"size": size, "checksum": sum(map(sum, rows))}


@queue.task(type="io", priority="high", timeout=30)
def resize_image(s3_url: str, width: int = 800, height: int = 600):
    """Download and resize an image (I/O-bound, runs in a thread pool)."""
    time.sleep(2)  # simulate network I/O
    return {"status": "success", "new_url": "s3://bucket/resized.jpg", "size": f"{width}x{height}"}


@queue.task
async def notify_user(user_id: int, message: str = "Your export is ready"):
    """Send a push notification (async, runs on the worker's event loop)."""
    await asyncio.sleep(0.2)
    return {"user_id": user_id, "delivered": True}


@queue.task(type="io", cron="*/5 * * * *")
def health_check_db():
    """Runs every 5 minutes, exactly once across the whole worker fleet."""
    return {"db": "healthy"}


# 3. Queue some work:  python example_app.py
if __name__ == "__main__":
    print(matrix_multiply.delay_sync(200))
    print(notify_user.delay_sync(42, message="Hello from PyTaskQ"))
