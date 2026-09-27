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
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# The W3C propagator knows how to serialize/deserialize trace context
# into a simple dict with a "traceparent" key.
_propagator = TraceContextTextMapPropagator()


def init_tracer(service_name: str) -> trace.Tracer:
    """
    Initialize OpenTelemetry and return a Tracer for this service.

    Args:
        service_name: Identifies this process in Jaeger UI.
                      Use "pytaskq-api" for app.py, "pytaskq-worker" for worker.py.

    The OTLP exporter sends spans to Jaeger over gRPC (port 4317).
    The endpoint is read from OTEL_EXPORTER_OTLP_ENDPOINT env var,
    defaulting to localhost for local dev.
    """
    # Resource = metadata that Jaeger attaches to every span from this process
    resource = Resource.create({"service.name": service_name})

    # TracerProvider = the "factory" that creates Tracers
    provider = TracerProvider(resource=resource)

    # BatchSpanProcessor = collects spans in memory and ships them to Jaeger
    # in batches every 5 seconds in a background thread.
    # This is why there's ZERO latency impact on your API responses.
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))

    # Set this as the global TracerProvider
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
