"""
test_task_queuing.py — Tests for the Producer side (app.py)
============================================================

WHAT WE TEST HERE:
    The "enqueue" contract: when an API endpoint is called, does the task
    land in the correct priority queue in Redis with the correct shape?

WHY THIS MATTERS:
    If the shape is wrong (missing task_id, wrong key names), the worker
    will fail to validate the task and it'll be logged as "Failed".
    These tests catch that before it ever reaches the worker.

ARCHITECTURE TESTED:
    CLIENT → app.py → Redis queue:{priority}
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
    # Monkeypatch get_client_ip to return a known, stable IP for tests
    monkeypatch.setattr(app, "get_client_ip", lambda req: "127.0.0.1")

    # ASGITransport lets httpx talk to FastAPI without a real network/port
    transport = ASGITransport(app=app.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, fake_redis_for_app


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: Task is enqueued to the correct priority queue with correct shape
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_default_priority_task_pushed_to_queue_default(test_client):
    """
    SCENARIO: Client posts a task with no priority specified (defaults to "default")
    EXPECT:   Task appears in queue:default with correct fields
    """
    client, r = test_client

    response = await client.post("/task/enqueue", json={
        "task_name": "matrix_multiply",
        "args": [5],
    })

    # 1. HTTP response must be 200 and contain task_id + status
    assert response.status_code == 200
    body = response.json()
    assert "task_id" in body
    assert body["status"] == "queued"

    # 2. Exactly ONE item must be in queue:default
    queue_length = await r.llen("queue:default")
    assert queue_length == 1, f"Expected 1 task in queue:default, got {queue_length}"

    # 3. Other priority queues must be empty
    assert await r.llen("queue:high") == 0
    assert await r.llen("queue:low") == 0

    # 4. The item must be valid JSON with the right shape
    raw = await r.lindex("queue:default", 0)
    task = json.loads(raw)
    assert task["task_name"] == "matrix_multiply"
    assert task["args"] == [5]
    assert task["retry_count"] == 0
    assert task["priority"] == "default"
    assert "task_id" in task
    assert "client_ip" in task
    assert task["task_id"] == body["task_id"]   # API response ID matches queue ID


@pytest.mark.asyncio
async def test_high_priority_task_pushed_to_queue_high(test_client):
    """
    SCENARIO: Client posts a task with priority="high"
    EXPECT:   Task appears in queue:high, NOT in queue:default or queue:low
    """
    client, r = test_client

    response = await client.post("/task/enqueue", json={
        "task_name": "matrix_multiply",
        "args": [5],
        "priority": "high",
    })

    assert response.status_code == 200

    # Must be in queue:high
    assert await r.llen("queue:high") == 1
    assert await r.llen("queue:default") == 0
    assert await r.llen("queue:low") == 0

    raw = await r.lindex("queue:high", 0)
    task = json.loads(raw)
    assert task["priority"] == "high"


@pytest.mark.asyncio
async def test_low_priority_task_pushed_to_queue_low(test_client):
    """
    SCENARIO: Client posts a task with priority="low"
    EXPECT:   Task appears in queue:low
    """
    client, r = test_client

    response = await client.post("/task/enqueue", json={
        "task_name": "matrix_multiply",
        "args": [5],
        "priority": "low",
    })

    assert response.status_code == 200
    assert await r.llen("queue:low") == 1
    assert await r.llen("queue:high") == 0
    assert await r.llen("queue:default") == 0


@pytest.mark.asyncio
async def test_each_task_gets_unique_id(test_client):
    """
    SCENARIO: Two requests come in simultaneously
    EXPECT:   Each gets a different task_id (no collision)

    WHY: UUID collisions would cause one task's result to overwrite another's
         in the result hash store.
    """
    client, r = test_client

    r1 = await client.post("/task/enqueue", json={"task_name": "matrix_multiply", "args": [3]})
    r2 = await client.post("/task/enqueue", json={"task_name": "matrix_multiply", "args": [3]})

    id1 = r1.json()["task_id"]
    id2 = r2.json()["task_id"]

    assert id1 != id2, "Two tasks were assigned the same ID — UUID collision!"
    assert await r.llen("queue:default") == 2


@pytest.mark.asyncio
async def test_task_payload_includes_client_ip(test_client):
    """
    SCENARIO: Client submits a task
    EXPECT:   The task JSON in Redis contains a client_ip field

    WHY: Per-IP multi-tenancy depends on every task carrying the sender's IP.
    """
    client, r = test_client

    await client.post("/task/enqueue", json={
        "task_name": "matrix_multiply",
        "args": [5],
    })

    raw = await r.lindex("queue:default", 0)
    task = json.loads(raw)
    assert "client_ip" in task, "Task is missing client_ip — multi-tenancy broken"


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
"""
    Test file written.
"""
