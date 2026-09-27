"""
PyTaskQ Benchmark Locustfile
============================
Purpose: Measure RAW API throughput and latency.
- Rate limit is raised to 10,000 req/min in app.py for this test.
- Uses unique UUIDs to avoid 409 dedup hits.
- Focuses on the enqueue endpoint (lightweight Redis write) — the core operation.

Run steps:
  1. Push raised rate limit to server + docker-compose up --build -d
  2. python -m locust -f tests/load/benchmark.py
  3. Open http://localhost:8089
  4. Run 4 separate tests: 10, 50, 100, 200 users (2 min each)
  5. Record p50, p99, RPS, error% for each
"""

import uuid
import random
from locust import HttpUser, task, between

API_KEY = "Bearer sk_YbA3K_xlKr3LZVIUx28pt7XV70aEcenO"
HOST    = "http://130.210.43.191:8000"


class BenchmarkUser(HttpUser):
    host     = HOST
    wait_time = between(0.05, 0.2)

    def on_start(self):
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": API_KEY,
        }

    @task(8)
    def enqueue_fast(self):
        """
        Fast enqueue: small matrix (10x10) = near-zero CPU cost on the worker.
        Measures pure API + Redis write throughput.
        UUID idempotency_key ensures dedup never triggers.
        """
        payload = {
            "task_name": "matrix_multiply",
            "args": [10],
            "priority": "default",
            "idempotency_key": str(uuid.uuid4()),
        }
        with self.client.post(
            "/task/enqueue",
            json=payload,
            headers=self.headers,
            name="enqueue (fast)",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 409):
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:80]}")

    @task(2)
    def enqueue_heavy(self):
        """
        Heavy enqueue: large matrix (200x200).
        Measures how backpressure from a saturated worker affects enqueue latency.
        """
        payload = {
            "task_name": "matrix_multiply",
            "args": [200],
            "priority": "high",
            "idempotency_key": str(uuid.uuid4()),
        }
        with self.client.post(
            "/task/enqueue",
            json=payload,
            headers=self.headers,
            name="enqueue (heavy)",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 409):
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:80]}")

    @task(1)
    def health_check(self):
        """Baseline: measures overhead of a zero-work endpoint."""
        with self.client.get(
            "/health",
            headers=self.headers,
            name="health",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")
