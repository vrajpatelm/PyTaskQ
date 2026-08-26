"""
conftest.py — Shared fixtures for all tests
============================================

WHY THIS FILE EXISTS:
    pytest automatically loads conftest.py before any test file in its directory.
    Fixtures defined here are available to every test without importing them.

WHAT IS A FIXTURE?
    A fixture is a reusable setup function decorated with @pytest.fixture.
    Tests declare what they need in their function arguments, pytest injects them.

WHY DO WE MOCK yagmail AND task_registery HERE?
    task_registery.py imports yagmail at module level and connects to SMTP.
    worker.py imports task_registery at module level.
    So the moment any test does `import worker`, it would crash with:
        ModuleNotFoundError: No module named 'yagmail'
    OR try to connect to a real email server.

    The solution: use sys.modules to inject a fake version of the problematic
    modules BEFORE any test ever imports worker. This is called "module-level mocking".

    This is done in a pytest_configure hook (runs before any test collection).
"""

import sys
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


# ── Module-level mocking — runs before test collection ────────────────────────
# We inject fake modules into sys.modules so that when worker.py does
#   from task_registery import TASKS
# it gets our fake TASKS dict instead of the real one that needs yagmail.

def pytest_configure(config):
    """
    pytest_configure is a special hook called once at startup.
    We use it to pre-inject mocked modules before any test imports worker.
    """
    # 1. Mock yagmail itself (the missing package)
    mock_yagmail = MagicMock()
    mock_yagmail.SMTP.return_value = MagicMock()
    sys.modules["yagmail"] = mock_yagmail

    # 2. Mock task_registery with safe stub functions
    #    These stubs behave like the real functions but:
    #    - Don't send real emails
    #    - Don't do heavy matrix math (tests use mocked executor anyway)
    def fake_send_email(email, subject, body):
        return {"result": f"Fake email sent to {email}"}

    def fake_matrix_multiply(size):
        return [[float(i * size + j) for j in range(size)] for i in range(size)]

    mock_task_registery = MagicMock()
    mock_task_registery.TASKS = {
        "send_email": {"handler": fake_send_email, "type": "io"},
        "matrix_multiply": {"handler": fake_matrix_multiply, "type": "cpu"},
    }
    sys.modules["task_registery"] = mock_task_registery


# ── Redis fixture ─────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def fake_redis():
    """
    Provides a fresh, empty in-memory Redis instance for each test.

    WHY PER-TEST:
        Each test gets its own clean Redis. No test can affect another's data.
        This is called "test isolation" — critical for reliable tests.
    """
    r = fakeredis.FakeRedis(decode_responses=True)
    yield r
    await r.flushall()   # wipe everything after the test
    await r.aclose()


# ── Executor fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def thread_pool():
    """Real ThreadPoolExecutor — used in tests that exercise actual task dispatch."""
    pool = ThreadPoolExecutor(max_workers=2)
    yield pool
    pool.shutdown(wait=True)


@pytest.fixture
def process_pool():
    """Real ProcessPoolExecutor — used in tests that exercise CPU-bound dispatch."""
    pool = ProcessPoolExecutor(max_workers=1)
    yield pool
    pool.shutdown(wait=True)


# ── Sample task data ──────────────────────────────────────────────────────────

@pytest.fixture
def sample_email_task():
    """
    Returns a valid email task dict — the exact shape app.py sends to Redis.
    Centralizing this means if the task format ever changes, fix it here once.
    """
    return {
        "task_id": "test-uuid-email-001",
        "task_name": "send_email",
        "args": ["test@example.com", "Hello", "Test body"],
        "retry_count": 0,
    }


@pytest.fixture
def sample_matrix_task():
    """Returns a valid matrix_multiply task dict."""
    return {
        "task_id": "test-uuid-matrix-001",
        "task_name": "matrix_multiply",
        "args": [5],   # small size so tests are fast
        "retry_count": 0,
    }


@pytest.fixture
def exhausted_task():
    """
    A task that has already been retried 3 times.
    Any further failure should send it to the dead_letter_queue.
    """
    return {
        "task_id": "test-uuid-dlq-001",
        "task_name": "send_email",
        "args": ["test@example.com", "Subject", "Body"],
        "retry_count": 3,
    }
