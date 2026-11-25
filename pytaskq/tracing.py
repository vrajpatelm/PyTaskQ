"""
pytaskq.tracing — OpenTelemetry tracing, from the request that queues a job to the worker that runs it
=======================================================================================================

Off by default. Set OTEL_EXPORTER_OTLP_ENDPOINT (e.g. http://jaeger:4317) and
install the extra (`pip install "pytaskq[tracing]"`, already in the Docker
image) to send traces to Jaeger, Grafana Tempo, Honeycomb, Datadog…

What one trace shows:

    POST /task/enqueue                    API request        (pytaskq-api)
      └─ enqueue send_email               job queued         (pytaskq-api, or your app)
           └─ run send_email              attempt 1          (pytaskq-worker)
           └─ run send_email              attempt 2, retry   (pytaskq-worker)
                └─ enqueue _deliver_webhook                  (follow-up job)
                     └─ run _deliver_webhook

The trace context travels inside the job as `trace_carrier` (a job is JSON, with no HTTP
headers); the worker continues the same trace. `task.delay()` inside your own span, and a
`traceparent` header sent to the API, join your trace.

Privacy: spans never record URLs (invite links carry secrets in their path), query strings,
headers, request bodies or job arguments.
"""

from __future__ import annotations

import os
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# The W3C propagator serialises trace context as a `traceparent` string.
_propagator = TraceContextTextMapPropagator()
_tracer = trace.get_tracer("pytaskq")
_api_tracer = trace.get_tracer("pytaskq-api")


def tracing_requested() -> bool:
    return bool(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")) or \
        os.getenv("PYTASKQ_TRACING", "").lower() in ("1", "true", "yes")


def is_enabled() -> bool:
    """True when spans are actually recorded (ours, or a provider your own app installed)."""
    provider = trace.get_tracer_provider()
    return not isinstance(provider, (trace.ProxyTracerProvider, trace.NoOpTracerProvider))


def init_tracer(service_name: str) -> trace.Tracer:
    """
    Set up exporting for this process ("pytaskq-api" or "pytaskq-worker") and return a tracer.

    Does nothing unless OTEL_EXPORTER_OTLP_ENDPOINT (or PYTASKQ_TRACING=1) is set,
    so a fresh install doesn't log "connection refused" when no collector runs.
    Safe to call more than once, and leaves a provider your own app installed alone.
    """
    if not tracing_requested() or is_enabled():
        return trace.get_tracer(service_name)
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        import logging
        logging.getLogger("pytaskq").warning(
            'Tracing is configured but not installed. Run: pip install "pytaskq[tracing]"')
        return trace.get_tracer(service_name)

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "") or "http://localhost:4317"
    # OTEL_SERVICE_NAME wins if you set it; OTEL_RESOURCE_ATTRIBUTES is merged in.
    resource = Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME") or service_name})
    provider = TracerProvider(resource=resource)
    # Plain-text gRPC only for http:// endpoints (a local Jaeger); https:// uses TLS.
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=endpoint.startswith("http://"))
    # Spans are sent in batches from a background thread: no added request latency.
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name)


def inject_trace_context() -> dict:
    """The current trace context as {"traceparent": ...}, or {} when nothing is being traced."""
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier


def extract_trace_context(carrier: dict):
    """Turn a job's `trace_carrier` back into a parent context (None for an empty carrier)."""
    if not carrier or not isinstance(carrier, dict):
        return None
    return _propagator.extract(carrier=carrier)


@contextmanager
def producer_span(payload: dict):
    """
    Wrap queueing a job in an "enqueue <task>" span and write its context into the job.

    Without tracing this is a no-op and the job carries no trace data.
    """
    name = payload.get("task_name", "task")
    with _tracer.start_as_current_span(
        f"enqueue {name}", kind=SpanKind.PRODUCER,
        attributes={"task.name": name, "task.id": payload.get("task_id", "")},
    ) as span:
        if not payload.get("trace_carrier"):
            carrier = inject_trace_context()
            if carrier:
                payload["trace_carrier"] = carrier
        yield span


class TracingMiddleware:
    """
    A span for every API request, continuing the caller's `traceparent` if it sent one.

    Plain ASGI (no buffering), and it steps aside entirely when tracing is off.
    Named after the matched route ("GET /task/{task_id}"), never the raw URL.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not is_enabled() or scope.get("path", "").startswith("/static/"):
            await self.app(scope, receive, send)
            return

        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope.get("headers") or []}
        parent = _propagator.extract(carrier=headers)
        method = scope.get("method", "GET")
        status: dict = {}

        async def send_and_note_status(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        with _api_tracer.start_as_current_span(
            method, context=parent, kind=SpanKind.SERVER, attributes={"http.request.method": method},
        ) as span:
            try:
                await self.app(scope, receive, send_and_note_status)
            finally:
                template = getattr(scope.get("route"), "path", None)
                if template:
                    span.update_name(f"{method} {template}")
                    span.set_attribute("http.route", template)
                code = status.get("code")
                if code:
                    span.set_attribute("http.response.status_code", code)
                    if code >= 500:
                        span.set_status(Status(StatusCode.ERROR))
