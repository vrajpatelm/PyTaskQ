from pytaskq import PyTaskQ
import time
import asyncio

# 1. Initialize the queue
queue = PyTaskQ()

# 2. Define your tasks!
@queue.task(type="cpu")
def matrix_multiply(size):
    """Heavy CPU task - will run in ProcessPoolExecutor"""
    print(f"Multiplying a {size}x{size} matrix...")
    return [[float(i * size + j) for j in range(size)] for i in range(size)]

@queue.task(type="io")
def resize_image(s3_url, width, height):
    """IO bound task - will run in ThreadPoolExecutor"""
    print(f"Downloading {s3_url} and resizing to {width}x{height}")
    time.sleep(2) # Simulate network IO
    return {"status": "success", "new_url": "s3://bucket/resized.jpg"}

@queue.task(type="io", cron="*/5 * * * *")
def health_check_db():
    """Runs every 5 minutes automatically across the cluster"""
    print("Checking database health...")
    return {"db": "healthy"}
