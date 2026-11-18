"""
test_tracing.py — one trace from the request that queues a job to every attempt that runs it
=============================================================================================

Spans are captured in memory (no collector needed):

  * POST /task/enqueue  →  enqueue <task>  →  run <task>  all share one trace id,
    continuing a `traceparent` the caller sent
  * every retry attempt and a webhook follow-up job join the same trace
  * the SDK (`task.delay()`) inside your own span joins your trace
  * with tracing off, jobs carry no trace data at all
  * spans never contain URLs, secrets from the path, or job arguments
  * init_tracer: off without an endpoint, TLS for https endpoints, never
    replaces a provider your own app installed
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import httpx
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.util._once import Once

import pytaskq.worker as worker
from pytaskq import PyTaskQ, tracing
from pytaskq.core import auth, queueing
from pytaskq.core.config import settings

UPSTREAM_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
UPSTREAM = f"00-{UPSTREAM_TRACE}-00f067aa0ba902b7-01"


def _reset_globals():
    """OpenTelemetry allows one global provider per process; tests need a fresh one each time."""
    trace._TRACER_PROVIDER_SET_ONCE = Once()
    trace._TRACER_PROVIDER = None
    for t in (tracing._tracer, tracing._api_tracer, worker.tracer):
        if hasattr(t, "_real_tracer"):
            t._real_tracer = None


@pytest.fixture
def spans():
    _reset_globals()
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    yield exporter
    _reset_globals()


@pytest.fixture
async def client(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"), \
         patch.object(worker.redis_client, "r", r):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            yield c


def hexid(n: int) -> str:
    return format(n, "032x")


async def run_job(r, raw: str, tasks: dict):
    bak = worker.WORKER_ID
    worker.WORKER_ID = "test-worker"
    await r.lpush("processing_queue:test-worker", raw)
    pool = ThreadPoolExecutor(max_workers=1)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    try:
        with patch.object(worker, "TASKS", tasks):
            await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, pool)
    finally:
        pool.shutdown(wait=True)
        worker.WORKER_ID = bak


async def test_request_queue_and_worker_share_one_trace(spans, client, r):
    res = await client.post("/task/enqueue", json={"task_name": "send_email", "kwargs": {"to": "ada@example.com"}},
                            headers={"traceparent": UPSTREAM})
    assert res.status_code == 200
    raw = await r.rpop("queue:default")
    assert json.loads(raw)["trace_carrier"]["traceparent"].split("-")[1] == UPSTREAM_TRACE

    await run_job(r, raw, {"send_email": {"handler": lambda to: "sent", "type": "io"}})

    by_name = {s.name: s for s in spans.get_finished_spans()}
    server, producer, consumer = by_name["POST /task/enqueue"], by_name["enqueue send_email"], by_name["run send_email"]
    assert {hexid(s.context.trace_id) for s in (server, producer, consumer)} == {UPSTREAM_TRACE}
    assert server.parent.span_id == int("00f067aa0ba902b7", 16), "continues the caller's trace"
    assert [s.name for s in spans.get_finished_spans()].count("POST /task/enqueue") == 1, "one request span, not two"
    assert producer.parent.span_id == server.context.span_id
    assert consumer.parent.span_id == producer.context.span_id
    assert server.kind == trace.SpanKind.SERVER and producer.kind == trace.SpanKind.PRODUCER
    assert consumer.kind == trace.SpanKind.CONSUMER
    assert server.attributes["http.route"] == "/task/enqueue"
    assert server.attributes["http.response.status_code"] == 200
    assert consumer.attributes["task.status"] == "Success"
    assert consumer.attributes["task.id"] == res.json()["task_id"]


async def test_retries_and_webhook_follow_ups_join_the_trace(spans, client, r):
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("first try fails")
        return "ok"

    payload = queueing.new_payload("flaky", on_success_url="https://hooks.example.com/done")
    with tracing._tracer.start_as_current_span("my app"):
        await queueing.enqueue(r, payload)
    raw = await r.rpop("queue:default")
    tasks = {"flaky": {"handler": flaky, "type": "io"}}
    await run_job(r, raw, tasks)                                    # attempt 1 fails → retry scheduled
    retry_raw = (await r.zrange("delayed_tasks", 0, -1))[0]
    await run_job(r, retry_raw, tasks)                              # attempt 2 succeeds → webhook queued

    finished = spans.get_finished_spans()
    trace_ids = {hexid(s.context.trace_id) for s in finished}
    assert len(trace_ids) == 1, [s.name for s in finished]
    runs = [s for s in finished if s.name == "run flaky"]
    assert [s.attributes["task.status"] for s in runs] == ["RetryScheduled", "Success"]
    assert runs[0].status.status_code == trace.StatusCode.ERROR
    assert any(s.name == "enqueue _deliver_webhook" and s.parent.span_id == runs[1].context.span_id for s in finished)
    webhook_raw = await r.rpop("queue:default")
    assert json.loads(webhook_raw)["trace_carrier"], "the follow-up job carries the trace on to its worker"


async def test_sdk_inside_your_own_span_joins_your_trace(spans, r):
    app = PyTaskQ()

    @app.task(type="io")
    def export(report_id):
        pass

    with patch("pytaskq.core.redis_client.r", r):
        with tracing._tracer.start_as_current_span("POST /checkout") as mine:
            await export.delay(5)
    raw = await r.rpop("queue:default")
    carried = json.loads(raw)["trace_carrier"]["traceparent"]
    assert carried.split("-")[1] == hexid(mine.get_span_context().trace_id)


async def test_without_tracing_jobs_carry_nothing(client, r):
    _reset_globals()
    assert not tracing.is_enabled()
    assert (await client.post("/task/enqueue", json={"task_name": "x"}, headers={"traceparent": UPSTREAM})).status_code == 200
    payload = json.loads(await r.rpop("queue:default"))
    assert "trace_carrier" not in payload or payload["trace_carrier"] == {}


async def test_spans_never_contain_secrets_or_arguments(spans, client, r):
    invite = await auth.create_invite(r, "viewer", created_by="admin")
    secret = invite[0]
    with patch.object(settings, "PYTASKQ_AUTH", "on"):
        await client.get(f"/auth/invites/{secret}")
        await client.get(f"/auth/invites/{secret}?also=query")
    await client.post("/task/enqueue", json={"task_name": "x", "kwargs": {"password": "hunter2"}})
    dump = json.dumps([{"name": s.name, "attrs": dict(s.attributes or {})} for s in spans.get_finished_spans()])
    assert secret not in dump and "hunter2" not in dump and "query" not in dump
    assert "GET /auth/invites/{token}" in dump


async def test_server_errors_mark_the_span(spans, client, r):
    from redis.exceptions import ConnectionError as RedisConnectionError

    async def down(*a, **k):
        raise RedisConnectionError("Connection refused")

    with patch.object(r, "execute_command", down):
        assert (await client.get("/tasks")).status_code == 503
    span = next(s for s in spans.get_finished_spans() if s.name == "GET /tasks")
    assert span.status.status_code == trace.StatusCode.ERROR


def test_init_tracer_is_off_without_an_endpoint(monkeypatch):
    _reset_globals()
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("PYTASKQ_TRACING", raising=False)
    tracing.init_tracer("pytaskq-test")
    assert not tracing.is_enabled()


@pytest.mark.parametrize("endpoint, insecure", [("http://jaeger:4317", True), ("https://api.honeycomb.io:443", False)])
def test_init_tracer_uses_tls_for_https(monkeypatch, endpoint, insecure):
    _reset_globals()
    seen = {}

    class FakeExporter:
        def __init__(self, **kwargs):
            seen.update(kwargs)

        def export(self, spans):
            return None

        def shutdown(self):
            pass

        def force_flush(self, timeout_millis=None):
            return True

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
    monkeypatch.setenv("OTEL_SERVICE_NAME", "billing-workers")
    monkeypatch.setattr("opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter", FakeExporter)
    try:
        tracing.init_tracer("pytaskq-worker")
        assert seen == {"endpoint": endpoint, "insecure": insecure}
        provider = trace.get_tracer_provider()
        assert provider.resource.attributes["service.name"] == "billing-workers"
    finally:
        _reset_globals()


def test_init_tracer_keeps_your_own_provider(spans, monkeypatch):
    mine = trace.get_tracer_provider()
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
    tracing.init_tracer("pytaskq-api")
    assert trace.get_tracer_provider() is mine
