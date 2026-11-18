"""
src/tracing.py — Centralized OpenTelemetry setup for PyTaskQ
=============================================================

WHY THIS FILE EXISTS:
  Both app.py (API) and worker.py (consumer) need to emit traces.
  Instead of duplicating OTel setup code, they both call:
      tracer = init_tracer("pytaskq-api")   # or "pytaskq-worker"

HOW TRACES CROSS THE REDIS QUEUE:
  Normal HTTP tracing works because libraries inject a `traceparent` header
  automatically. But Redis LPUSH is just a raw string — there are no headers.

  Our solution:
    1. app.py:    inject_trace_context()  → returns {"traceparent": "00-abc..."}
    2. We stuff that dict into the task JSON payload as `trace_carrier`
    3. worker.py: extract_trace_context(carrier) → reconstructs the parent span
    4. Worker creates a CHILD span linked to the same Trace ID

  Result: Jaeger shows one continuous waterfall from API → Queue → Worker.
"""

import os
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# The W3C propagator knows how to serialize/deserialize trace context
# into a simple dict with a "traceparent" key.
_propagator = TraceContextTextMapPropagator()


def init_tracer(service_name: str) -> trace.Tracer:
    """
    Return a Tracer for this service ("pytaskq-api" or "pytaskq-worker").

    Tracing is OPT-IN: nothing is exported unless OTEL_EXPORTER_OTLP_ENDPOINT
    (or PYTASKQ_TRACING=1) is set. Without it, spans are no-ops, so a fresh
    install doesn't spam "connection refused" errors when Jaeger isn't running.
    """
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    enabled = bool(endpoint) or os.getenv("PYTASKQ_TRACING", "").lower() in ("1", "true", "yes")
    if not enabled:
        return trace.get_tracer(service_name)

    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    except ImportError:  # exporter not installed → keep no-op tracing
        return trace.get_tracer(service_name)

    # Resource = metadata that Jaeger attaches to every span from this process
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    # BatchSpanProcessor ships spans in a background thread — no request latency.
    exporter = OTLPSpanExporter(endpoint=endpoint or "http://localhost:4317", insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    return trace.get_tracer(service_name)


def inject_trace_context() -> dict:
    """
    Serialize the CURRENT active span's context into a plain dict.

    Called by app.py right after starting a span. The returned dict
    looks like: {"traceparent": "00-<trace_id>-<span_id>-01"}

    We store this dict in the task JSON payload so the worker can
    reconstruct it later.

    Think of it like writing your return address on a letter —
    whoever receives it knows where the conversation started.
    """
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier


def extract_trace_context(carrier: dict) -> trace.Context | None:
    """
    Deserialize a trace context dict back into an OTel Context object.

    Called by worker.py when it picks up a task from Redis.
    The returned Context is passed to tracer.start_as_current_span(context=...)
    so the worker's span becomes a CHILD of the API's span.

    If the carrier is empty (old tasks without tracing), returns None
    and the worker just creates an independent trace.

    Think of it like reading the return address off a letter —
    now you can reply in the same conversation thread.
    """
    if not carrier:
        return None
    return _propagator.extract(carrier=carrier)
