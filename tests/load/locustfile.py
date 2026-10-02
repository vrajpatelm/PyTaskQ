"""
PyTaskQ Load Test — realistic mixed workload
============================================

All configuration comes from environment variables (loaded from .env):

    LOADTEST_HOST    — target base URL       (default: http://localhost:8000)
    LOADTEST_API_KEY — a valid tenant API key (default: dummy, expect 401s)
    LOADTEST_USERS   — tenant count to simulate (default: 100)

Setup:
    1. Get a key:  POST /admin/keys/create  (Authorization: Bearer <MASTER_KEY>)
    2. Put it in .env:  LOADTEST_API_KEY=sk_xxx
    3. Run:  python -m locust -f tests/load/locustfile.py --host=$LOADTEST_HOST

Every request carries a unique Idempotency-Key so the content-hash dedup
(layer 2) never returns 409 for legitimately different work.
"""

import json
import os
import random
import uuid

from dotenv import load_dotenv
from locust import HttpUser, between, task

load_dotenv()

# ── Configuration (env-driven, nothing hardcoded) ─────────────────────────────
LOADTEST_HOST = os.getenv("LOADTEST_HOST", "http://localhost:8000")
LOADTEST_API_KEY = os.getenv("LOADTEST_API_KEY", "sk_set_LOADTEST_API_KEY_in_env")


class PyTaskQUser(HttpUser):
    host = LOADTEST_HOST
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
            "Authorization": f"Bearer {LOADTEST_API_KEY}",
        }

    @task(3)  # Weight 3: This task runs 3x more often than others
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

    @task(5)  # Weight 5: Very common operation
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

    @task(1)  # Weight 1: Less frequent operation
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
