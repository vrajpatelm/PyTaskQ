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

from conftest import AUTH_HEADERS, TEST_API_KEY, TEST_TENANT_ID


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

    # Seed a real API key so app.authenticate's Redis lookup works against
    # fakeredis — tests exercise the actual auth dependency, not a stub.
    await fake_redis_for_app.hset(f"api_key:{TEST_API_KEY}", mapping={
        "tenant_id": TEST_TENANT_ID,
        "label": "test-suite",
    })

    # ASGITransport lets httpx talk to FastAPI without a real network/port
    transport = ASGITransport(app=app.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, fake_redis_for_app


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: Task is enqueued to the correct priority queue with correct shape
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_request_without_api_key_is_rejected(test_client):
    """
    SCENARIO: Anonymous client posts a task (no Authorization header)
    EXPECT:   Rejected with 401/403 — the queue is never touched.

    WHY: Unauthenticated submissions would let anyone enqueue work
         and bypass per-tenant rate limits.
    """
    client, r = test_client

    response = await client.post("/task/enqueue", json={
        "task_name": "matrix_multiply",
        "args": [5],
    })
    assert response.status_code in (401, 403)
    assert await r.llen("queue:default") == 0


@pytest.mark.asyncio
async def test_request_with_invalid_api_key_is_rejected(test_client):
    """
    SCENARIO: Client presents a key that does not exist in Redis
    EXPECT:   401 Unauthorized.
    """
    client, r = test_client

    response = await client.post(
        "/task/enqueue",
        json={"task_name": "matrix_multiply", "args": [5]},
        headers={"Authorization": "Bearer sk_definitely_not_a_real_key"},
    )
    assert response.status_code == 401
    assert await r.llen("queue:default") == 0


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
    }, headers=AUTH_HEADERS)

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
    assert task["tenant_id"] == TEST_TENANT_ID, "Task must carry its tenant for isolation"
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
    }, headers=AUTH_HEADERS)

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
    }, headers=AUTH_HEADERS)

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

    # Different args → different Layer-2 content hash, so dedup won't interfere
    r1 = await client.post("/task/enqueue", json={"task_name": "matrix_multiply", "args": [3]}, headers=AUTH_HEADERS)
    r2 = await client.post("/task/enqueue", json={"task_name": "matrix_multiply", "args": [4]}, headers=AUTH_HEADERS)

    id1 = r1.json()["task_id"]
    id2 = r2.json()["task_id"]

    assert id1 != id2, "Two tasks were assigned the same ID — UUID collision!"
    assert await r.llen("queue:default") == 2


@pytest.mark.asyncio
async def test_identical_resubmit_within_window_is_rejected(test_client):
    """
    SCENARIO: The SAME task_name + args + tenant is submitted twice quickly
              with no Idempotency-Key header
    EXPECT:   First request enqueues; second gets 409 from the Layer-2
              content-hash safety net.

    WHY: Double-clicks and auto-retries must not silently create duplicate
         work when the client forgot to send an idempotency key.
    """
    client, r = test_client

    payload = {"task_name": "matrix_multiply", "args": [7]}
    first = await client.post("/task/enqueue", json=payload, headers=AUTH_HEADERS)
    assert first.status_code == 200

    second = await client.post("/task/enqueue", json=payload, headers=AUTH_HEADERS)
    assert second.status_code == 409
    assert await r.llen("queue:default") == 1  # duplicate did NOT enqueue


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
    }, headers=AUTH_HEADERS)

    raw = await r.lindex("queue:default", 0)
    task = json.loads(raw)
    assert "client_ip" in task, "Task is missing client_ip — multi-tenancy broken"


@pytest.mark.asyncio
async def test_result_endpoint_returns_empty_for_unknown_id(test_client):
    """
    SCENARIO: Client polls /task/{id} for a task that doesn't exist yet
    EXPECT:   404 with a clean error, not a 500.

    WHY: Client might poll before the worker finishes; the API must
         distinguish "not yet" from "server error".
    """
    client, r = test_client

    response = await client.get("/task/nonexistent-uuid-000", headers=AUTH_HEADERS)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_result_endpoint_enforces_tenant_isolation(test_client):
    """
    SCENARIO: A task hash exists in Redis but was stored by ANOTHER tenant
              (or has no tenant_id at all — legacy/poisoned data).
    EXPECT:   403 — one tenant must never read another tenant's results.

    WHY: Task IDs are UUIDs, but security cannot depend on them being
         unguessable. Fail closed on any tenant mismatch.
    """
    client, r = test_client

    # Another tenant's task result sitting in Redis
    await r.hset("task:foreign-task-001", mapping={
        "task_id": "foreign-task-001",
        "status": "Success",
        "tenant_id": "t_someone_else",
        "result": "secret output",
    })
    # A legacy/poisoned hash with NO tenant field
    await r.hset("task:no-tenant-task", mapping={
        "task_id": "no-tenant-task",
        "status": "Success",
        "result": "orphan output",
    })

    resp_foreign = await client.get("/task/foreign-task-001", headers=AUTH_HEADERS)
    assert resp_foreign.status_code == 403
    resp_orphan = await client.get("/task/no-tenant-task", headers=AUTH_HEADERS)
    assert resp_orphan.status_code == 403


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 7: Idempotency-key lifecycle (header AND body keys)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_body_idempotency_key_response_is_cached(test_client):
    """
    SCENARIO: Client sends the idempotency key in the BODY (not the header),
              then retries the identical request.
    EXPECT:   The retry returns the cached first response (same task_id) —
              a 200, NOT a 409.

    WHY: store_idempotency_response used to read ONLY the header, so
         body-supplied keys were never finalized: the claim stayed "pending"
         and every retry was 409 until the TTL lapsed a day later.
    """
    client, r = test_client

    payload = {
        "task_name": "matrix_multiply",
        "args": [11],
        "idempotency_key": "body-key-001",
    }
    first = await client.post("/task/enqueue", json=payload, headers=AUTH_HEADERS)
    assert first.status_code == 200

    second = await client.post("/task/enqueue", json=payload, headers=AUTH_HEADERS)
    assert second.status_code == 200, (
        "body-key retry was not finalized — client is stuck seeing 409"
    )
    assert second.json()["task_id"] == first.json()["task_id"]
    assert await r.llen("queue:default") == 1, "retry must NOT enqueue a second task"


@pytest.mark.asyncio
async def test_header_idempotency_key_response_is_cached(test_client):
    """
    SCENARIO: Same as above, but the key arrives in the Idempotency-Key header.
    EXPECT:   Retry returns the cached response; only one task is enqueued.

    WHY: Guards the header path against regressions while the body path was fixed.
    """
    client, r = test_client

    payload = {"task_name": "matrix_multiply", "args": [12]}
    headers = {**AUTH_HEADERS, "Idempotency-Key": "header-key-001"}

    first = await client.post("/task/enqueue", json=payload, headers=headers)
    assert first.status_code == 200

    second = await client.post("/task/enqueue", json=payload, headers=headers)
    assert second.status_code == 200
    assert second.json()["task_id"] == first.json()["task_id"]
    assert await r.llen("queue:default") == 1


@pytest.mark.asyncio
async def test_stale_pending_claim_is_short_lived_and_reclaimable(test_client, monkeypatch):
    """
    SCENARIO: The process dies between claiming the key and finalizing it, so
              the claim is left as "pending" in Redis.
    EXPECT:   1) the pending claim has a SHORT ttl (not the full 24h response ttl)
              2) an immediate retry gets 409 + Retry-After
              3) once the claim lapses, the next retry is accepted as fresh.

    WHY: A long-lived pending tombstone turns a crash into a permanent 409
         for that key. Short TTL + NX gives free reclaim.
    """
    import app
    client, r = test_client

    # Simulate "crash before finalize": the claim is made but never completed.
    async def never_finalized(*args, **kwargs):
        return None

    monkeypatch.setattr(app, "store_idempotency_response", never_finalized)

    payload = {
        "task_name": "matrix_multiply",
        "args": [13],
        "idempotency_key": "crash-key-001",
    }
    first = await client.post("/task/enqueue", json=payload, headers=AUTH_HEADERS)
    assert first.status_code == 200

    key = f"idempotency:{TEST_TENANT_ID}:crash-key-001"
    assert await r.get(key) == "pending"
    ttl = await r.ttl(key)
    assert 0 < ttl <= app.IDEMPOTENCY_PENDING_TTL, (
        f"pending claim ttl is {ttl}s — must be short, not the response TTL"
    )

    # While still pending, a duplicate is rejected with a Retry-After hint.
    second = await client.post("/task/enqueue", json=payload, headers=AUTH_HEADERS)
    assert second.status_code == 409
    assert second.headers.get("retry-after") == str(app.IDEMPOTENCY_PENDING_TTL)

    # The claim lapses (TTL expiry) → the same key can be claimed again.
    await r.delete(key)
    third = await client.post("/task/enqueue", json=payload, headers=AUTH_HEADERS)
    assert third.status_code == 200, "lapsed claim must be reclaimable, not a dead end"
"""
    Test file written.
"""
