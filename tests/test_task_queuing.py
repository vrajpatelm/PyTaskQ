"""
test_task_queuing.py — Tests for the Producer side (app.py)
============================================================

WHAT WE TEST HERE:
    The "enqueue" contract: when an API endpoint is called, does the task
    land in Redis with the correct shape?

WHY THIS MATTERS:
    If the shape is wrong (missing task_id, wrong key names), the worker
    will fail to validate the task and it'll be logged as "Failed".
    These tests catch that before it ever reaches the worker.

ARCHITECTURE TESTED:
    CLIENT → app.py → Redis task_queue
"""

import json
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from httpx import AsyncClient, ASGITransport


# ── We need to patch app.py's Redis BEFORE importing app ──────────────────────
# app.py creates `r = redis.Redis(...)` at import time. We must swap that
# with fakeredis before the module loads. We do that with pytest monkeypatching.

@pytest_asyncio.fixture
async def fake_redis_for_app():
    """Fake Redis instance that will be injected into app.py."""
    r = fakeredis.FakeRedis(decode_responses=True)
    yield r
    await r.flushall()
    await r.aclose()


@pytest_asyncio.fixture
async def test_client(fake_redis_for_app, monkeypatch):
    """
    Creates a real FastAPI test client with Redis swapped out for fakeredis.

    HOW monkeypatch WORKS:
        monkeypatch.setattr(module, "attribute", new_value)
        It temporarily replaces the attribute for the duration of the test,
        then restores it automatically. No manual cleanup needed.
    """
    import app  # import AFTER monkeypatching below
    monkeypatch.setattr(app, "r", fake_redis_for_app)

    # ASGITransport lets httpx talk to FastAPI without a real network/port
    transport = ASGITransport(app=app.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, fake_redis_for_app


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: Task is enqueued with correct shape
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_matrix_task_is_pushed_to_queue(test_client):
    """
    SCENARIO: Client calls GET /task/mul?size=5
    EXPECT:   One task appears in task_queue with correct fields
    """
    client, r = test_client

    response = await client.get("/task/mul?size=5")

    # 1. HTTP response must be 200 and contain task_id + status
    assert response.status_code == 200
    body = response.json()
    assert "task_id" in body
    assert body["status"] == "queued"

    # 2. Exactly ONE item must be in the Redis queue
    queue_length = await r.llen("task_queue")
    assert queue_length == 1, f"Expected 1 task in queue, got {queue_length}"

    # 3. The item must be valid JSON with the right shape
    raw = await r.lindex("task_queue", 0)
    task = json.loads(raw)
    assert task["task_name"] == "matrix_multiply"
    assert task["args"] == [5]
    assert task["retry_count"] == 0
    assert "task_id" in task
    assert task["task_id"] == body["task_id"]   # API response ID matches queue ID


@pytest.mark.asyncio
async def test_email_task_is_pushed_to_queue(test_client):
    """
    SCENARIO: Client POSTs to /task/send_email
    EXPECT:   One task in queue with correct email args
    """
    client, r = test_client

    response = await client.post("/task/send_email", data={
        "email": "vraj@example.com",
        "title": "Hello",
        "body": "Test body"
    })

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "queued"

    raw = await r.lindex("task_queue", 0)
    task = json.loads(raw)
    assert task["task_name"] == "send_email"
    assert task["args"][0] == "vraj@example.com"
    assert task["args"][1] == "Hello"
    assert task["args"][2] == "Test body"
    assert task["retry_count"] == 0


@pytest.mark.asyncio
async def test_each_task_gets_unique_id(test_client):
    """
    SCENARIO: Two requests come in simultaneously
    EXPECT:   Each gets a different task_id (no collision)

    WHY: UUID collisions would cause one task's result to overwrite another's
         in the result hash store.
    """
    client, r = test_client

    r1 = await client.get("/task/mul?size=3")
    r2 = await client.get("/task/mul?size=3")

    id1 = r1.json()["task_id"]
    id2 = r2.json()["task_id"]

    assert id1 != id2, "Two tasks were assigned the same ID — UUID collision!"
    assert await r.llen("task_queue") == 2


@pytest.mark.asyncio
async def test_matrix_default_size_is_10(test_client):
    """
    SCENARIO: Client calls /task/mul with NO size parameter
    EXPECT:   args defaults to [10]
    """
    client, r = test_client

    await client.get("/task/mul")   # no ?size=

    raw = await r.lindex("task_queue", 0)
    task = json.loads(raw)
    assert task["args"] == [10], f"Default size should be 10, got {task['args']}"


@pytest.mark.asyncio
async def test_result_endpoint_returns_empty_for_unknown_id(test_client):
    """
    SCENARIO: Client polls /task/{id} for a task that doesn't exist yet
    EXPECT:   Returns empty result dict, not an error

    WHY: Client might poll before worker finishes. Should get {} not 500.
    """
    client, r = test_client

    response = await client.get("/task/nonexistent-uuid-000")
    assert response.status_code == 200
    assert response.json() == {"result": {}}
