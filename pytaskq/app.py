"""
pytaskq.app — the PyTaskQ SDK
==============================

    from pytaskq import PyTaskQ

    queue = PyTaskQ()

    @queue.task                                  # plain decorator works
    def send_welcome_email(user_id): ...

    @queue.task(type="cpu", max_retries=5, timeout=60, priority="high")
    def render_report(report_id, fmt="pdf"): ...

    @queue.task                                  # async def is detected automatically
    async def call_api(url): ...

    @queue.task(cron="*/5 * * * *")              # runs every 5 minutes on the fleet
    def cleanup(): ...

    # Enqueue from async code
    await render_report.delay(42, fmt="csv")
    await render_report.apply_async(args=[42], priority="low", countdown=30)

    # …or from plain sync code (scripts, Django/Flask views)
    render_report.delay_sync(42)

    # Inspect results
    await queue.status(task_id)
    await queue.wait(task_id, timeout=30)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import os
import time
from typing import Any, Callable, Optional

from pytaskq.core import queueing
from pytaskq.core.config import settings
from pytaskq.net import SAFE_OPENER, assert_safe_url

TASK_TYPES = ("cpu", "io", "async")

# Option names `delay()` treats as queue options rather than task kwargs —
# unless the task function itself declares a parameter with that name.
_DELAY_OPTIONS = ("priority", "on_success_url")


def _redis():
    # Looked up at call time so tests (and apps) can swap the client.
    from pytaskq.core import redis_client
    return redis_client.r


def _describe_params(func: Callable) -> list[dict]:
    """JSON-able description of the task's parameters (drives dashboard forms)."""
    params = []
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return params
    for p in sig.parameters.values():
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            params.append({"name": p.name, "kind": "var_positional" if p.kind == p.VAR_POSITIONAL else "var_keyword",
                           "required": False})
            continue
        info: dict[str, Any] = {"name": p.name, "required": p.default is p.empty}
        if p.default is not p.empty:
            try:
                json.dumps(p.default)
                info["default"] = p.default
            except (TypeError, ValueError):
                info["default"] = repr(p.default)
        if p.annotation is not p.empty:
            ann = p.annotation
            info["annotation"] = ann if isinstance(ann, str) else getattr(ann, "__name__", str(ann))
        params.append(info)
    return params


def describe_tasks(tasks: dict[str, dict]) -> dict[str, dict]:
    """JSON-able description of a TASKS registry dict."""
    out = {}
    for task_name, entry in tasks.items():
        fn = entry["handler"]
        doc = inspect.getdoc(fn) or ""
        out[task_name] = {
            "type": entry.get("type", "cpu"),
            "cron": entry.get("cron"),
            "priority": entry.get("priority", "default"),
            "max_retries": entry.get("max_retries") if entry.get("max_retries") is not None else settings.MAX_RETRIES,
            "timeout": entry.get("timeout") if entry.get("timeout") is not None else settings.TASK_TIMEOUT,
            "doc": doc.split("\n\n")[0].strip(),
            "params": _describe_params(fn),
            "internal": task_name.startswith("_"),
        }
    return out


class PyTaskQ:
    """A registry of background tasks plus the helpers to enqueue them."""

    def __init__(
        self,
        name: str = "pytaskq",
        *,
        default_priority: str = "default",
        max_retries: Optional[int] = None,
        timeout: Optional[float] = None,
        include_builtins: bool = True,
    ):
        if default_priority not in queueing.PRIORITIES:
            raise ValueError(f"default_priority must be one of {queueing.PRIORITIES}")
        self.name = name
        self.default_priority = default_priority
        self.max_retries = max_retries
        self.timeout = timeout
        self.TASKS: dict[str, dict] = {}
        if include_builtins:
            self.task(name="_deliver_webhook", type="io")(_deliver_webhook)

    # ── registration ─────────────────────────────────────────────────────────
    def task(
        self,
        func: Optional[Callable] = None,
        *,
        name: Optional[str] = None,
        type: Optional[str] = None,
        cron: Optional[str] = None,
        priority: Optional[str] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[float] = None,
    ):
        """
        Register a function as a background task. Usable as `@queue.task` or
        `@queue.task(...)`.

        type        "cpu" (process pool), "io" (thread pool) or "async" (event loop).
                    Defaults to "async" for `async def`, otherwise "cpu".
        cron        Standard 5-field cron expression; the fleet enqueues it on schedule.
        priority    Default queue for this task: "high", "default" or "low".
        max_retries Attempts after the first failure before dead-lettering (default 3).
        timeout     Seconds before the attempt is abandoned (default TASK_TIMEOUT).
        """

        if isinstance(func, str):  # legacy positional form: @queue.task("io")
            type, func = func, None

        def decorator(fn: Callable) -> Callable:
            is_async = inspect.iscoroutinefunction(fn)
            task_type = type or ("async" if is_async else "cpu")
            if task_type not in TASK_TYPES:
                raise ValueError(f"type must be one of {TASK_TYPES}, got {task_type!r}")
            if is_async and task_type != "async":
                raise ValueError(f"{fn.__name__} is `async def`; use type='async' (or leave type unset)")
            if task_type == "async" and not is_async:
                raise ValueError(f"{fn.__name__} has type='async' but is not an `async def` function")
            prio = priority or self.default_priority
            if prio not in queueing.PRIORITIES:
                raise ValueError(f"priority must be one of {queueing.PRIORITIES}, got {prio!r}")
            if cron is not None:
                import croniter
                if not croniter.croniter.is_valid(cron):
                    raise ValueError(f"Invalid cron expression for {fn.__name__}: {cron!r}")
            if max_retries is not None and max_retries < 0:
                raise ValueError("max_retries must be >= 0")

            task_name = name or fn.__name__
            if task_name in self.TASKS and self.TASKS[task_name]["handler"] is not fn:
                raise ValueError(f"A task named {task_name!r} is already registered")

            retries = max_retries if max_retries is not None else self.max_retries
            limit = timeout if timeout is not None else self.timeout
            self.TASKS[task_name] = {
                "handler": fn,
                "type": task_type,
                "cron": cron,
                "priority": prio,
                "max_retries": retries,
                "timeout": limit,
            }
            own_params = set(inspect.signature(fn).parameters)

            def _split(kwargs: dict) -> tuple[dict, dict]:
                opts = {k: kwargs.pop(k) for k in _DELAY_OPTIONS if k in kwargs and k not in own_params}
                return kwargs, opts

            async def apply_async(args=None, kwargs=None, *, priority: Optional[str] = None,
                                  countdown: float = 0, on_success_url: Optional[str] = None,
                                  task_id: Optional[str] = None) -> dict:
                payload = queueing.new_payload(
                    task_name, args, kwargs,
                    priority=priority or prio,
                    on_success_url=on_success_url,
                    max_retries=retries,
                    timeout=limit,
                    task_id=task_id,
                )
                return await queueing.enqueue(_redis(), payload, delay_seconds=countdown)

            async def delay(*args, **kwargs) -> dict:
                kwargs, opts = _split(kwargs)
                return await apply_async(args, kwargs, **opts)

            async def schedule(*args, delay_seconds: float = 60, **kwargs) -> dict:
                kwargs, opts = _split(kwargs)
                return await apply_async(args, kwargs, countdown=delay_seconds, **opts)

            def delay_sync(*args, **kwargs) -> dict:
                """Enqueue from synchronous code (no running event loop)."""
                kwargs, opts = _split(kwargs)
                return _run_sync(lambda r: queueing.enqueue(
                    r,
                    queueing.new_payload(task_name, args, kwargs, priority=opts.get("priority") or prio,
                                         on_success_url=opts.get("on_success_url"),
                                         max_retries=retries, timeout=limit),
                ))

            fn.delay = delay
            fn.schedule = schedule
            fn.apply_async = apply_async
            fn.delay_sync = delay_sync
            fn.task_name = task_name
            return fn

        if func is not None and callable(func):
            return decorator(func)
        return decorator

    # ── introspection ────────────────────────────────────────────────────────
    def specs(self) -> dict[str, dict]:
        """Describe registered tasks (published to Redis for the dashboard)."""
        return describe_tasks(self.TASKS)

    async def publish(self) -> None:
        """Advertise this app's tasks so the dashboard and API can see them."""
        await queueing.publish_registry(_redis(), self.specs())

    # ── results ──────────────────────────────────────────────────────────────
    async def status(self, task_id: str) -> Optional[dict]:
        """Current status hash of a task (None if unknown or expired)."""
        meta = await _redis().hgetall(queueing.task_key(task_id))
        return queueing.public_view(meta) if meta else None

    async def wait(self, task_id: str, timeout: float = 30, poll_interval: float = 0.25) -> dict:
        """Wait until the task reaches a final state; raises TimeoutError otherwise."""
        deadline = time.monotonic() + timeout
        while True:
            info = await self.status(task_id)
            if info and info.get("status") in queueing.FINAL_STATES:
                return info
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Task {task_id} did not finish within {timeout}s")
            await asyncio.sleep(poll_interval)

    async def cancel(self, task_id: str) -> bool:
        """Cancel a task that has not started yet."""
        ok, _ = await queueing.cancel(_redis(), task_id)
        return ok

    def __repr__(self) -> str:
        return f"<PyTaskQ {self.name!r} tasks={sorted(self.TASKS)}>"


def _run_sync(coro_factory):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("delay_sync() was called inside a running event loop; use `await task.delay(...)` instead")

    async def runner():
        from pytaskq.core.redis_client import make_client
        client = make_client()
        try:
            return await coro_factory(client)
        finally:
            await client.aclose()

    return asyncio.run(runner())


# ── Built-in task: reliable webhook delivery ────────────────────────────────────
# Enqueued by the worker after a task with `on_success_url` succeeds. It goes
# through the queue like any other task, so it gets retries and the DLQ.
def _deliver_webhook(url: str, task_id: str, task_name: str, result: str):
    """Deliver a signed POST to a task's on_success_url."""
    assert_safe_url(url)

    payload = json.dumps({
        "event": "task.success",
        "task_id": task_id,
        "task_name": task_name,
        "result": result,
    }).encode("utf-8")

    # Optional HMAC-SHA256 signing if WEBHOOK_SECRET is set
    secret = os.environ.get("WEBHOOK_SECRET", "")
    signature = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest() if secret else "unsigned"

    import urllib.request
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "PyTaskQ-Webhook/1.0",
            "X-PyTaskQ-Signature": f"sha256={signature}",
            "X-PyTaskQ-Task-ID": task_id,
        },
        method="POST",
    )
    with SAFE_OPENER.open(req, timeout=10) as resp:
        status = resp.status
        if status < 200 or status >= 300:
            raise RuntimeError(f"Webhook delivery failed: target returned HTTP {status}")
    return {"delivered_to": url, "status_code": status}
