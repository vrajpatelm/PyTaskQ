from locust import HttpUser, task, between, events
import json
import random
import uuid

class PyTaskQUser(HttpUser):
    host = "http://130.210.43.191:8000"
    # Simulate realistic network delays between user actions (0.1 to 1 second)
    wait_time = between(0.1, 1.0)

    def on_start(self):
        """
        Executed when a simulated user starts.
        We assign a fake 'tenant_id' to simulate multi-tenancy.
        """
        self.tenant_id = f"tenant_{random.randint(1, 100)}"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer sk_YbA3K_xlKr3LZVIUx28pt7XV70aEcenO" # Replace if you enforce auth in prod
        }

    @task(3) # Weight 3: This task runs 3x more often than others
    def enqueue_cpu_task(self):
        """Simulate enqueuing a heavy math task"""
        matrix_size = random.randint(10, 500)
        payload = {
            "task_name": "matrix_multiply",
            "args": [matrix_size],
            "priority": "default",
            "idempotency_key": str(uuid.uuid4()),  # unique per request — avoids false 409
        }
        
        # Name the endpoint explicitly so Locust groups the URLs in the UI
        self.client.post(
            "/task/enqueue", 
            json=payload, 
            headers=self.headers,
            name="/task/enqueue (CPU)"
        )

    @task(5) # Weight 5: Very common operation
    def enqueue_io_task(self):
        """Simulate enqueuing a quick string task"""
        payload = {
            "task_name": "generate_csv_report",
            "args": [random.randint(10, 500)],
            "priority": random.choice(["high", "default", "low"]),
            "idempotency_key": str(uuid.uuid4()),
        }
        self.client.post(
            "/task/enqueue", 
            json=payload, 
            headers=self.headers,
            name="/task/enqueue (I/O)"
        )

    @task(1) # Weight 1: Less frequent operation
    def schedule_delayed_task(self):
        """Simulate scheduling a task for the future"""
        payload = {
            "task_name": "url_health_check",
            "args": ["https://httpbin.org/get"],
            "priority": "default",
            "idempotency_key": str(uuid.uuid4()),  # unique — prevents dedup 409
        }
        self.client.post(
            "/task/schedule?delay_seconds=10", 
            json=payload, 
            headers=self.headers,
            name="/task/schedule"
        )
