import json
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor


API_URL = "http://130.210.43.191:8000"
NUM_TASKS = 50
MATRIX_SIZE = 180


def enqueue_single_task(task_num: int) -> bool:
    try:
        req = urllib.request.Request(
            f"{API_URL}/task/mul?size={MATRIX_SIZE}",
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        return False


def get_metrics() -> dict:
    try:
        req = urllib.request.Request(f"{API_URL}/metrics")
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode("utf-8"))
    except Exception:
        pass
    return {}


def main():
    print("=================================================")
    print(f"🚀 Starting Stress Test: {NUM_TASKS} Heavy Matrix Tasks (size={MATRIX_SIZE})...")
    print("=================================================")

    start_time = time.time()

    # 1. Dispatch all tasks concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(enqueue_single_task, range(NUM_TASKS)))

    success_count = sum(1 for r in results if r)
    dispatch_time = time.time() - start_time

    print(f"✅ Enqueued {success_count}/{NUM_TASKS} tasks successfully in {dispatch_time:.2f}s!")
    print("\n📊 Live Metrics Monitor (Press Ctrl+C to stop)")
    print("-" * 55)
    print(f"{'Pending':<12} | {'Processing':<12} | {'Delayed':<12} | {'DLQ':<12}")
    print("-" * 55)

    # Short grace period for Redis queues to populate
    time.sleep(0.2)

    # 2. Poll metrics live until queue is empty
    empty_count = 0
    while True:
        metrics = get_metrics()
        if metrics:
            pending = metrics.get("pending", 0)
            processing = metrics.get("processing", 0)
            delayed = metrics.get("delayed", 0)
            dlq = metrics.get("dlq", 0)

            sys.stdout.write(f"\r{pending:<12} | {processing:<12} | {delayed:<12} | {dlq:<12}")
            sys.stdout.flush()

            if pending == 0 and processing == 0:
                empty_count += 1
                if empty_count >= 3:  # Confirm queue is empty for 3 consecutive checks
                    print("\n" + "-" * 55)
                    total_time = time.time() - start_time
                    print(f"🎉 Stress Test Completed in {total_time:.2f} seconds!")
                    print(f"⚡ Throughput: {NUM_TASKS / total_time:.2f} tasks/sec")
                    break
            else:
                empty_count = 0
        else:
            sys.stdout.write("\rWaiting for API response...                      ")
            sys.stdout.flush()

        time.sleep(0.4)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
