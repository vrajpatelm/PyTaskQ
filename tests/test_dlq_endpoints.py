"""
test_dlq_endpoints.py — Tests for the DLQ management & metrics API endpoints
=============================================================================

WHAT WE TEST HERE:
    The 5 endpoints in app.py:
      - GET  /metrics              → live per-user queue counts
      - GET  /dlq                  → list all tasks in dlq:{client_ip}
      - POST /dlq/replay/{task_id} → move task from DLQ back to queue:{priority}
      - POST /dlq/purge/{task_id}  → permanently delete one task from DLQ
      - POST /dlq/purge_all        → clear the entire DLQ for this user

ARCHITECTURE TESTED:
    CLIENT → app.py → Redis queues (per-IP isolated)

HOW WE ISOLATE app.py:
    Same pattern as test_task_queuing.py:
      - Import app AFTER monkeypatching Redis with fakeredis
      - Use httpx AsyncClient with ASGITransport (no real HTTP port needed)
      - Each test gets a fresh, empty Redis — no shared state between tests

WHAT IS "DLQ"?
    dlq:{client_ip} is a per-tenant Redis List.
    Tasks end up there after 3 failed retries.
    An operator (or this dashboard) can then:
      - Inspect what failed
      - Replay (re-queue) the task to its original priority queue
      - Purge (permanently delete) the task
"""

import json
import time
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from httpx import AsyncClient, ASGITransport

from conftest import AUTH_HEADERS, TEST_API_KEY, TEST_TENANT_ID


# The IP that our monkeypatched get_client_ip will return
TEST_CLIENT_IP = "127.0.0.1"


# ── Shared fixtures ───────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def fake_redis_for_app():
    """Fresh in-memory Redis for each test."""
    r = fakeredis.FakeRedis(decode_responses=True)
    yield r
    await r.flushall()
    await r.aclose()


@pytest_asyncio.fixture
async def test_client(fake_redis_for_app, monkeypatch):
    """
    FastAPI test client with Redis swapped for fakeredis.

    monkeypatch replaces app.r at test time and auto-restores it afterwards.
    """
    import app
    monkeypatch.setattr(app, "r", fake_redis_for_app)
    # Monkeypatch get_client_ip to return a known, stable IP for tests
    monkeypatch.setattr(app, "get_client_ip", lambda req: TEST_CLIENT_IP)

    # Seed a real API key so app.authenticate's Redis lookup works
    await fake_redis_for_app.hset(f"api_key:{TEST_API_KEY}", mapping={
        "tenant_id": TEST_TENANT_ID,
        "label": "test-suite",
    })

    transport = ASGITransport(app=app.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, fake_redis_for_app


def make_dlq_task(task_id, task_name="send_email", priority="default"):
    """Build a realistic DLQ task payload — same shape worker.py pushes."""
    return json.dumps({
        "task_id": task_id,
        "task_name": task_name,
        "args": ["a@b.com", "Hello", "Body"],
        "retry_count": 3,
        "priority": priority,
        "tenant_id": TEST_TENANT_ID,
        "client_ip": TEST_CLIENT_IP,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1:  GET /metrics
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_metrics_all_zero_on_empty_system(test_client):
    """
    SCENARIO: No tasks have been submitted yet — all queues are empty.
    EXPECT:   All metric counts are 0.

    WHY: The dashboard must render zeros correctly, not error or show None.
    """
    client, r = test_client

    response = await client.get("/metrics", headers=AUTH_HEADERS)
    assert response.status_code == 200

    data = response.json()
    assert data["pending"] == 0,    "pending counter should be 0"
    assert data["processing"] == 0, "processing counter should be 0"
    assert data["delayed"] == 0,    "delayed counter should be 0"
    assert data["dlq"] == 0,        "dlq should be empty"


@pytest.mark.asyncio
async def test_metrics_reflects_per_ip_counters(test_client):
    """
    SCENARIO: Stats counters are set for a specific IP, DLQ has tasks.
    EXPECT:   /metrics returns the exact counts for that IP.

    WHY: The dashboard must accurately reflect live per-user system state.
    """
    client, r = test_client

    # Set per-IP stats counters (simulating what app.py/worker.py do)
    await r.set(f"stats:pending:{TEST_TENANT_ID}", 3)
    await r.set(f"stats:processing:{TEST_TENANT_ID}", 1)
    await r.set(f"stats:delayed:{TEST_TENANT_ID}", 0)
    await r.set(f"stats:completed:{TEST_TENANT_ID}", 10)
    await r.set(f"stats:failed:{TEST_TENANT_ID}", 2)
    for i in range(2):
        await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task(f"dlq-{i}"))

    response = await client.get("/metrics", headers=AUTH_HEADERS)
    assert response.status_code == 200

    data = response.json()
    assert data["pending"] == 3
    assert data["processing"] == 1
    assert data["delayed"] == 0
    assert data["dlq"] == 2
    assert data["completed_total"] == 10
    assert data["failed_total"] == 2


@pytest.mark.asyncio
async def test_metrics_isolated_between_users(test_client):
    """
    SCENARIO: Stats exist for a DIFFERENT IP than the test client.
    EXPECT:   /metrics returns 0 for the test client (not the other user's data).

    WHY: Multi-tenancy isolation — users must not see each other's metrics.
    """
    client, r = test_client

    # Set stats for a different IP
    await r.set("stats:completed:t_other_tenant", 500)
    await r.lpush("dlq:t_other_tenant", make_dlq_task("other-user-task"))

    response = await client.get("/metrics", headers=AUTH_HEADERS)
    data = response.json()
    assert data["completed_total"] == 0, "Should not see another user's completed count"
    assert data["dlq"] == 0, "Should not see another user's DLQ"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 2:  GET /dlq
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_dlq_returns_empty_list_when_no_failures(test_client):
    """
    SCENARIO: No tasks have failed — DLQ is empty.
    EXPECT:   Response is a list, length 0.

    WHY: Dashboard must render an empty DLQ gracefully (no 500 errors).
    """
    client, r = test_client

    response = await client.get("/dlq", headers=AUTH_HEADERS)
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data["tasks"], list)
    assert len(data["tasks"]) == 0


@pytest.mark.asyncio
async def test_dlq_lists_all_failed_tasks(test_client):
    """
    SCENARIO: 3 tasks are in the per-IP DLQ.
    EXPECT:   /dlq returns all 3, each as a parsed dict (not raw JSON strings).

    WHY: The dashboard needs to display task_id, task_name, retry_count etc.
         Returning raw JSON strings would require the client to double-parse.
    """
    client, r = test_client

    ids = ["failed-001", "failed-002", "failed-003"]
    for task_id in ids:
        await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task(task_id))

    response = await client.get("/dlq", headers=AUTH_HEADERS)
    assert response.status_code == 200

    data = response.json()
    returned_ids = {t["task_id"] for t in data["tasks"]}
    assert returned_ids == set(ids), (
        f"Expected task IDs {set(ids)}, got {returned_ids}"
    )


@pytest.mark.asyncio
async def test_dlq_task_has_required_fields(test_client):
    """
    SCENARIO: One task is in the DLQ.
    EXPECT:   The returned object has all fields the dashboard needs.

    WHY: If fields are missing, the UI will silently show blank values.
         Better to catch this at the API level.
    """
    client, r = test_client

    await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task("field-check-001"))

    response = await client.get("/dlq", headers=AUTH_HEADERS)
    task = response.json()["tasks"][0]

    assert "task_id" in task
    assert "task_name" in task
    assert "retry_count" in task
    assert "args" in task
    assert "priority" in task


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 3:  POST /dlq/replay/{task_id}
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_replay_moves_task_to_priority_queue(test_client):
    """
    SCENARIO: Operator clicks "Replay" on a failed task.
    EXPECT:   Task is removed from DLQ and added to its original priority queue.

    WHY: This is the core of DLQ recovery — the task must run again.
         If it stays in DLQ after replay, it's stuck forever.
    """
    client, r = test_client

    task_id = "replay-test-001"
    await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task(task_id, priority="default"))
    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 1

    response = await client.post(f"/dlq/replay/{task_id}", headers=AUTH_HEADERS)
    assert response.status_code == 200

    # DLQ must be empty after replay
    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 0, "Task was NOT removed from DLQ"

    # The task must be in the correct priority queue
    total_queued = (
        await r.llen("queue:high") +
        await r.llen("queue:default") +
        await r.llen("queue:low")
    )
    assert total_queued == 1, "Task was NOT added to any priority queue"

    # The replayed task must have retry_count reset to 0
    for queue_name in ["queue:high", "queue:default", "queue:low"]:
        raw = await r.lindex(queue_name, 0)
        if raw:
            replayed = json.loads(raw)
            assert replayed["retry_count"] == 0, (
                "Replayed task should have retry_count=0, not 3 "
                "(otherwise it would immediately go back to DLQ on first failure)"
            )
            break


@pytest.mark.asyncio
async def test_replay_nonexistent_task_returns_404(test_client):
    """
    SCENARIO: Operator tries to replay a task_id that isn't in DLQ.
    EXPECT:   404 Not Found response.

    WHY: Without this guard, a 200 OK on a no-op would silently mislead
         operators into thinking a task was replayed when nothing happened.
    """
    client, r = test_client

    response = await client.post(f"/dlq/replay/nonexistent-task-xyz", headers=AUTH_HEADERS)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_replay_only_removes_the_targeted_task(test_client):
    """
    SCENARIO: 3 tasks are in DLQ; operator replays ONE specific task.
    EXPECT:   Only that task is removed from DLQ; the other 2 remain.

    WHY: A bug could replay/remove all tasks instead of just the target.
         This test catches that regression.
    """
    client, r = test_client

    ids = ["keep-001", "replay-me", "keep-002"]
    for task_id in ids:
        await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task(task_id))

    response = await client.post(f"/dlq/replay/replay-me", headers=AUTH_HEADERS)
    assert response.status_code == 200

    # DLQ should still have exactly 2 tasks
    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 4:  POST /dlq/purge/{task_id}
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_purge_removes_task_from_dlq(test_client):
    """
    SCENARIO: Operator decides a task is unrecoverable and deletes it.
    EXPECT:   Task is gone from DLQ; no priority queues are touched.

    WHY: Purge is permanent deletion — NOT a replay. The task must
         not re-appear anywhere in the system after purge.
    """
    client, r = test_client

    task_id = "purge-test-001"
    await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task(task_id))

    response = await client.post(f"/dlq/purge/{task_id}", headers=AUTH_HEADERS)
    assert response.status_code == 200

    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 0, "Task was NOT purged from DLQ"
    assert await r.llen("queue:default") == 0, "Purge should NOT add task to any queue"


@pytest.mark.asyncio
async def test_purge_nonexistent_task_returns_404(test_client):
    """
    SCENARIO: Operator tries to purge a task_id that isn't in DLQ.
    EXPECT:   404 Not Found.
    """
    client, r = test_client

    response = await client.post("/dlq/purge/ghost-task-xyz", headers=AUTH_HEADERS)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_purge_does_not_affect_other_dlq_tasks(test_client):
    """
    SCENARIO: 3 tasks in DLQ; one is purged.
    EXPECT:   Only the targeted task disappears; others remain intact.
    """
    client, r = test_client

    ids = ["safe-001", "delete-me", "safe-002"]
    for task_id in ids:
        await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task(task_id))

    response = await client.post("/dlq/purge/delete-me", headers=AUTH_HEADERS)
    assert response.status_code == 200

    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 2
    remaining_raw = await r.lrange(f"dlq:{TEST_TENANT_ID}", 0, -1)
    remaining_ids = {json.loads(t)["task_id"] for t in remaining_raw}
    assert "delete-me" not in remaining_ids


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 5:  POST /dlq/purge_all
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_purge_all_clears_entire_dlq(test_client):
    """
    SCENARIO: 5 tasks are in DLQ; operator clicks "Purge All".
    EXPECT:   DLQ is completely empty; no priority queues are touched.

    WHY: This is the nuclear option — useful when the DLQ has accumulated
         hundreds of unrecoverable tasks from a bad deployment.
    """
    client, r = test_client

    for i in range(5):
        await r.lpush(f"dlq:{TEST_TENANT_ID}", make_dlq_task(f"dlq-task-{i}"))
    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 5

    response = await client.post("/dlq/purge_all", headers=AUTH_HEADERS)
    assert response.status_code == 200

    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 0, "DLQ was NOT fully cleared"
    assert await r.llen("queue:default") == 0, "purge_all should NOT touch any queue"


@pytest.mark.asyncio
async def test_purge_all_on_empty_dlq_succeeds(test_client):
    """
    SCENARIO: Operator accidentally clicks "Purge All" when DLQ is empty.
    EXPECT:   200 OK — idempotent operation, no error.

    WHY: UI might send the request even when count = 0.
         Should not crash or return 4xx/5xx.
    """
    client, r = test_client

    response = await client.post("/dlq/purge_all", headers=AUTH_HEADERS)
    assert response.status_code == 200
    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 0
