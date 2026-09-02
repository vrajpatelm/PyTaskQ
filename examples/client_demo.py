import time
import requests

BASE_URL = "http://localhost:8000"

def check_metrics():
    """Fetches queue sizes from the metrics endpoint."""
    try:
        response = requests.get(f"{BASE_URL}/metrics")
        if response.status_code == 200:
            print(f"[Metrics] Current state: {response.json()}")
        else:
            print(f"[Metrics] Failed to fetch metrics: {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("[Metrics] Failed to connect to API server. Is it running?")

def trigger_matrix_task(size=20):
    """Sends a matrix multiplication request to the API."""
    print(f"\n[Trigger] Dispatching CPU-bound task: matrix_multiply (size={size})...")
    try:
        response = requests.get(f"{BASE_URL}/task/mul", params={"size": size})
        if response.status_code == 200:
            data = response.json()
            print(f"[Trigger] Success! Task ID: {data['task_id']}, Status: {data['status']}")
            return data["task_id"]
        else:
            print(f"[Trigger] API returned error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[Trigger] Exception: {e}")
    return None

def trigger_email_task(recipient, subject, body):
    """Sends a send_email form request to the API."""
    print(f"\n[Trigger] Dispatching I/O-bound task: send_email to {recipient}...")
    try:
        response = requests.post(
            f"{BASE_URL}/task/send_email",
            data={"email": recipient, "title": subject, "body": body}
        )
        if response.status_code == 200:
            data = response.json()
            print(f"[Trigger] Success! Task ID: {data['task_id']}, Status: {data['status']}")
            return data["task_id"]
        else:
            print(f"[Trigger] API returned error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[Trigger] Exception: {e}")
    return None

def poll_task_status(task_id, max_retries=10, delay=1.5):
    """Polls the status of the given task ID until success or failure."""
    print(f"[Poll] Starting status checks for task {task_id}...")
    for i in range(max_retries):
        try:
            response = requests.get(f"{BASE_URL}/task/{task_id}")
            if response.status_code == 200:
                result_data = response.json().get("result", {})
                status = result_data.get("status")
                
                if not status:
                    print(f"[Poll] Attempt {i+1}: Task not processed yet...")
                elif status == "Success":
                    print(f"[Poll] Success! Result: {result_data.get('result')}")
                    return True
                elif status == "RetryScheduled":
                    print(f"[Poll] Task failed once; retry scheduled: {result_data.get('error')}")
                elif status == "DeadLetter":
                    print(f"[Poll] Task dead (DLQ)! Last error: {result_data.get('error')}")
                    return False
                else:
                    print(f"[Poll] Status: {status}")
            else:
                print(f"[Poll] Server returned status code: {response.status_code}")
        except Exception as e:
            print(f"[Poll] Exception during query: {e}")
        
        time.sleep(delay)
    print("[Poll] Max timeout reached. Task taking longer than expected.")
    return False

if __name__ == "__main__":
    print("=== PyTaskQ Client Demo ===")
    
    # 1. Print current queue metrics
    check_metrics()
    
    # 2. Dispatch a CPU task (matrix multiplication) and follow it
    matrix_id = trigger_matrix_task(size=30)
    if matrix_id:
        poll_task_status(matrix_id)
        
    # 3. Print metrics after task completion
    check_metrics()

    # 4. Dispatch an I/O task (email dispatch)
    email_id = trigger_email_task(
        recipient="test@example.com",
        subject="Integration Verification",
        body="Hello! This task was pushed from the example python client."
    )
    if email_id:
        # Note: Unless real SMTP email credentials are setup in .env,
        # this task will fail, schedule retries, and eventually land in the DLQ.
        # This acts as a great demonstration of the retry/DLQ pipeline!
        poll_task_status(email_id, max_retries=5, delay=2.0)

    # 5. Final metrics check
    check_metrics()
