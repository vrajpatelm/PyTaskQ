"""
conftest.py — Shared fixtures for the entire PyTaskQ test suite
===============================================================

WHY:
    - Provides a single fake-redis instance that every test file can use
      without touching a real Redis server.
    - fakeredis[lua] supports Lua scripting (eval/evalsha), which we need
      for FENCE_SCRIPT, REQUEUE_SCRIPT, and CRON_ENQUEUE_SCRIPT.
    - Each test gets a clean, flushed Redis state via autouse fixtures.

USAGE:
    Any test that declares `r` as a parameter gets the patched fake client.
    Any test that imports from pytaskq.worker automatically uses the patched
    redis_client because we patch it at the module level.
"""

import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from unittest.mock import patch


@pytest_asyncio.fixture
async def r():
    """
    A fully isolated fake Redis client per test.
    Supports Lua scripts (eval) via fakeredis[lua].
    """
    client = fakeredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture(autouse=True)
async def reset_worker_globals():
    """
    Reset module-level mutable state in worker.py before every test.

    WHY THIS IS NEEDED:
        worker.py uses module-level variables for:
          - shutdown_event  (asyncio.Event — persists across tests if not reset)
          - active_tasks    (set — tasks from one test bleed into the next)
          - TASKS           (dict — task registrations must not cross-pollinate)

        Without this, test order affects results — the classic 'it passes
        alone but fails in suite' problem.
    """
    import pytaskq.worker as worker
    worker.shutdown_event.clear()
    worker.active_tasks.clear()
    yield
    worker.shutdown_event.clear()
    worker.active_tasks.clear()
