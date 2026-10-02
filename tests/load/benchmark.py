"""
PyTaskQ Benchmark Locustfile
============================
Purpose: Measure RAW API throughput and latency.

All configuration comes from environment variables (loaded from .env):

    LOADTEST_HOST    — target base URL       (default: http://localhost:8000)
    LOADTEST_API_KEY — a valid tenant API key (default: dummy, expect 401s)

For benchmarking, raise the rate limit in .env (NOT in source):
    RATE_LIMIT_PER_MINUTE=10000

Run steps:
  1. Set env vars in .env, then start the stack:  docker-compose up --build -d
  2. python -m locust -f tests/load/benchmark.py --host=$LOADTEST_HOST
  3. Open http://localhost:8089
  4. Run 4 separate tests: 10, 50, 100, 200 users (2 min each)
  5. Record p50, p99, RPS, error% for each
"""

import os
import uuid

from dotenv import load_dotenv
from locust import HttpUser, between, task

load_dotenv()

# ── Configuration (env-driven, nothing hardcoded) ─────────────────────────────
LOADTEST_HOST = os.getenv("LOADTEST_HOST", "http://localhost:8000")
LOADTEST_API_KEY = os.getenv("LOADTEST_API_KEY", "sk_set_LOADTEST_API_KEY_in_env")


class BenchmarkUser(HttpUser):
    host = LOADTEST_HOST
    wait_time = between(0.05, 0.2)

    def on_start(self):
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {LOADTEST_API_KEY}",
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
