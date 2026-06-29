"""
test_dlq_endpoints.py — Tests for the DLQ management & metrics API endpoints
=============================================================================

WHAT WE TEST HERE:
    The 5 new endpoints you are implementing in app.py:
      - GET  /metrics              → live queue counts
      - GET  /dlq                  → list all tasks in dead_letter_queue
      - POST /dlq/replay/{task_id} → move task from DLQ back to task_queue
      - POST /dlq/purge/{task_id}  → permanently delete one task from DLQ
      - POST /dlq/purge_all        → clear the entire DLQ

ARCHITECTURE TESTED:
    CLIENT → app.py → Redis queues

HOW WE ISOLATE app.py:
    Same pattern as test_task_queuing.py:
      - Import app AFTER monkeypatching Redis with fakeredis
      - Use httpx AsyncClient with ASGITransport (no real HTTP port needed)
      - Each test gets a fresh, empty Redis — no shared state between tests

WHAT IS "DLQ"?
    dead_letter_queue is a Redis List.
    Tasks end up there after 3 failed retries.
    An operator (or this dashboard) can then:
      - Inspect what failed
      - Replay (re-queue) the task
      - Purge (permanently delete) the task
"""

import json
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from httpx import AsyncClient, ASGITransport


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

    transport = ASGITransport(app=app.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, fake_redis_for_app


def make_dlq_task(task_id, task_name="send_email"):
    """Build a realistic DLQ task payload — same shape worker.py pushes."""
    return json.dumps({
        "task_id": task_id,
        "task_name": task_name,
        "args": ["a@b.com", "Hello", "Body"],
        "retry_count": 3,
        "task_type": "io",
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

    response = await client.get("/metrics")
    assert response.status_code == 200

    data = response.json()
    assert data["pending"] == 0,    "task_queue should be empty"
    assert data["processing"] == 0, "processing_queue should be empty"
    assert data["delayed"] == 0,    "delayed_tasks ZSET should be empty"
    assert data["dlq"] == 0,        "dead_letter_queue should be empty"


@pytest.mark.asyncio
async def test_metrics_reflects_task_queue_count(test_client):
    """
    SCENARIO: 3 tasks are in task_queue, 1 in processing_queue, 2 in DLQ.
    EXPECT:   /metrics returns the exact counts.

    WHY: The dashboard must accurately reflect live system state.
         If metrics lie, operators won't know tasks are stuck.
    """
    client, r = test_client

    for i in range(3):
        await r.lpush("task_queue", make_dlq_task(f"pending-{i}"))
    await r.lpush("processing_queue", make_dlq_task("processing-1"))
    for i in range(2):
        await r.lpush("dead_letter_queue", make_dlq_task(f"dlq-{i}"))

    response = await client.get("/metrics")
    assert response.status_code == 200

    data = response.json()
    assert data["pending"] == 3
    assert data["processing"] == 1
    assert data["delayed"] == 0
    assert data["dlq"] == 2


@pytest.mark.asyncio
async def test_metrics_counts_delayed_tasks_from_zset(test_client):
    """
    SCENARIO: 2 tasks are sitting in the delayed_tasks sorted set.
    EXPECT:   metrics["delayed"] == 2

    WHY: delayed_tasks is a ZSET, not a list.
         The endpoint must use ZCARD, not LLEN, to count it correctly.
    """
    import time
    client, r = test_client

    future = time.time() + 60
    await r.zadd("delayed_tasks", {"task-a": future, "task-b": future})

    response = await client.get("/metrics")
    data = response.json()
    assert data["delayed"] == 2


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

    response = await client.get("/dlq")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data["tasks"], list)
    assert len(data["tasks"]) == 0


@pytest.mark.asyncio
async def test_dlq_lists_all_failed_tasks(test_client):
    """
    SCENARIO: 3 tasks are in dead_letter_queue.
    EXPECT:   /dlq returns all 3, each as a parsed dict (not raw JSON strings).

    WHY: The dashboard needs to display task_id, task_name, retry_count etc.
         Returning raw JSON strings would require the client to double-parse.
    """
    client, r = test_client

    ids = ["failed-001", "failed-002", "failed-003"]
    for task_id in ids:
        await r.lpush("dead_letter_queue", make_dlq_task(task_id))

    response = await client.get("/dlq")
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

    await r.lpush("dead_letter_queue", make_dlq_task("field-check-001"))

    response = await client.get("/dlq")
    task = response.json()["tasks"][0]

    assert "task_id" in task
    assert "task_name" in task
    assert "retry_count" in task
    assert "args" in task


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 3:  POST /dlq/replay/{task_id}
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_replay_moves_task_to_task_queue(test_client):
    """
    SCENARIO: Operator clicks "Replay" on a failed task.
    EXPECT:   Task is removed from DLQ and added to task_queue.

    WHY: This is the core of DLQ recovery — the task must run again.
         If it stays in DLQ after replay, it's stuck forever.
    """
    client, r = test_client

    task_id = "replay-test-001"
    await r.lpush("dead_letter_queue", make_dlq_task(task_id))
    assert await r.llen("dead_letter_queue") == 1
    assert await r.llen("task_queue") == 0

    response = await client.post(f"/dlq/replay/{task_id}")
    assert response.status_code == 200

    # DLQ must be empty after replay
    assert await r.llen("dead_letter_queue") == 0, "Task was NOT removed from DLQ"

    # task_queue must contain the task
    assert await r.llen("task_queue") == 1, "Task was NOT added to task_queue"

    # The replayed task must have retry_count reset to 0
    replayed = json.loads(await r.lindex("task_queue", 0))
    assert replayed["retry_count"] == 0, (
        "Replayed task should have retry_count=0, not 3 "
        "(otherwise it would immediately go back to DLQ on first failure)"
    )


@pytest.mark.asyncio
async def test_replay_nonexistent_task_returns_404(test_client):
    """
    SCENARIO: Operator tries to replay a task_id that isn't in DLQ.
    EXPECT:   404 Not Found response.

    WHY: Without this guard, a 200 OK on a no-op would silently mislead
         operators into thinking a task was replayed when nothing happened.
    """
    client, r = test_client

    response = await client.post("/dlq/replay/nonexistent-task-xyz")
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
        await r.lpush("dead_letter_queue", make_dlq_task(task_id))

    response = await client.post("/dlq/replay/replay-me")
    assert response.status_code == 200

    # DLQ should still have exactly 2 tasks
    assert await r.llen("dead_letter_queue") == 2
    assert await r.llen("task_queue") == 1


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 4:  POST /dlq/purge/{task_id}
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_purge_removes_task_from_dlq(test_client):
    """
    SCENARIO: Operator decides a task is unrecoverable and deletes it.
    EXPECT:   Task is gone from DLQ; task_queue stays empty (no re-queue).

    WHY: Purge is permanent deletion — NOT a replay. The task must
         not re-appear anywhere in the system after purge.
    """
    client, r = test_client

    task_id = "purge-test-001"
    await r.lpush("dead_letter_queue", make_dlq_task(task_id))

    response = await client.post(f"/dlq/purge/{task_id}")
    assert response.status_code == 200

    assert await r.llen("dead_letter_queue") == 0, "Task was NOT purged from DLQ"
    assert await r.llen("task_queue") == 0, "Purge should NOT add task to task_queue"


@pytest.mark.asyncio
async def test_purge_nonexistent_task_returns_404(test_client):
    """
    SCENARIO: Operator tries to purge a task_id that isn't in DLQ.
    EXPECT:   404 Not Found.
    """
    client, r = test_client

    response = await client.post("/dlq/purge/ghost-task-xyz")
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
        await r.lpush("dead_letter_queue", make_dlq_task(task_id))

    response = await client.post("/dlq/purge/delete-me")
    assert response.status_code == 200

    assert await r.llen("dead_letter_queue") == 2
    remaining_raw = await r.lrange("dead_letter_queue", 0, -1)
    remaining_ids = {json.loads(t)["task_id"] for t in remaining_raw}
    assert "delete-me" not in remaining_ids


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 5:  POST /dlq/purge_all
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_purge_all_clears_entire_dlq(test_client):
    """
    SCENARIO: 5 tasks are in DLQ; operator clicks "Purge All".
    EXPECT:   DLQ is completely empty; task_queue is untouched.

    WHY: This is the nuclear option — useful when the DLQ has accumulated
         hundreds of unrecoverable tasks from a bad deployment.
    """
    client, r = test_client

    for i in range(5):
        await r.lpush("dead_letter_queue", make_dlq_task(f"dlq-task-{i}"))
    assert await r.llen("dead_letter_queue") == 5

    response = await client.post("/dlq/purge_all")
    assert response.status_code == 200

    assert await r.llen("dead_letter_queue") == 0, "DLQ was NOT fully cleared"
    assert await r.llen("task_queue") == 0, "purge_all should NOT touch task_queue"


@pytest.mark.asyncio
async def test_purge_all_on_empty_dlq_succeeds(test_client):
    """
    SCENARIO: Operator accidentally clicks "Purge All" when DLQ is empty.
    EXPECT:   200 OK — idempotent operation, no error.

    WHY: UI might send the request even when count = 0.
         Should not crash or return 4xx/5xx.
    """
    client, r = test_client

    response = await client.post("/dlq/purge_all")
    assert response.status_code == 200
    assert await r.llen("dead_letter_queue") == 0
