from locust import HttpUser, task, between, events
import json
import random

class PyTaskQUser(HttpUser):
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
            "Authorization": "Bearer TEST_KEY" # Replace if you enforce auth in prod
        }

    @task(3) # Weight 3: This task runs 3x more often than others
    def enqueue_cpu_task(self):
        """Simulate enqueuing a heavy math task"""
        matrix_size = random.choice([50, 100, 200, 500])
        payload = {
            "task_name": "matrix_multiply",
            "args": [matrix_size],
            "priority": "default"
        }
        
        # Name the endpoint explicitly so Locust groups the URLs in the UI
        self.client.post(
            f"/task/enqueue?tenant_id={self.tenant_id}", 
            json=payload, 
            headers=self.headers,
            name="/task/enqueue (CPU)"
        )

    @task(5) # Weight 5: Very common operation
    def enqueue_io_task(self):
        """Simulate enqueuing a quick string task"""
        payload = {
            "task_name": "reverse_string",
            "args": ["hello load test " * 10],
            "priority": random.choice(["high", "default", "low"])
        }
        self.client.post(
            f"/task/enqueue?tenant_id={self.tenant_id}", 
            json=payload, 
            headers=self.headers,
            name="/task/enqueue (I/O)"
        )

    @task(1) # Weight 1: Less frequent operation
    def schedule_delayed_task(self):
        """Simulate scheduling a task for the future"""
        payload = {
            "task_name": "reverse_string",
            "args": ["delayed string"],
            "priority": "default"
        }
        self.client.post(
            f"/task/schedule?delay_seconds=10&tenant_id={self.tenant_id}", 
            json=payload, 
            headers=self.headers,
            name="/task/schedule"
        )
