"""
pytaskq.context — what a running task can ask about its own job
================================================================

Inside a task:

    from pytaskq import progress, cancelled, current_task_id

    @app.task(type="io")
    def import_rows(path):
        rows = load(path)
        for i, row in enumerate(rows):
            if cancelled():                 # someone pressed Cancel in the dashboard
                return "stopped early"
            save(row)
            progress(100 * (i + 1) / len(rows), f"{i + 1} of {len(rows)} rows")

`progress()` is cheap: the worker keeps only the latest value and writes it to
Redis about once a second, so calling it for every row is fine. It works in
every task type (CPU tasks send it from their process). Outside a job (calling
the function directly, in a test) these helpers do nothing: progress() returns
False, cancelled() returns False, current_task_id() returns None.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

MAX_MESSAGE = 200


@dataclass
class Job:
    task_id: str
    report: Callable[[str, Optional[float], Optional[str]], None]
    cancel: threading.Event = field(default_factory=threading.Event)
    stop: Optional[Callable[[], object]] = None      # set by the worker while the job runs
    left_running: Optional[object] = None            # an IO thread still running after a timeout/cancel


_current: contextvars.ContextVar[Optional[Job]] = contextvars.ContextVar("pytaskq_job", default=None)


def _check(percent, message) -> tuple[Optional[float], Optional[str]]:
    if percent is not None:
        if isinstance(percent, bool) or not isinstance(percent, (int, float)) or percent != percent:
            raise ValueError(f"progress() takes a percentage from 0 to 100, got {percent!r}")
        try:
            percent = float(percent)
        except OverflowError:            # an int too big for a float (break test D11)
            percent = 100.0 if percent > 0 else 0.0
        percent = round(min(100.0, max(0.0, percent)), 1)
    if message is not None:
        message = str(message)[:MAX_MESSAGE]
    return percent, message


def progress(percent: Optional[float] = None, message: Optional[str] = None) -> bool:
    """
    Report how far the running job has got: a percentage (0–100, clamped) and/or
    a short message (up to 200 characters). Shown in the dashboard and returned
    by `GET /task/{id}` / `app.status()` as `progress` and `progress_message`.
    Returns False when not called from inside a job.
    """
    job = _current.get()
    if job is None:
        return False
    percent, message = _check(percent, message)
    job.report(job.task_id, percent, message)
    return True


def cancelled() -> bool:
    """
    True once someone asked to cancel the running job. Async and CPU tasks are
    stopped for you; an IO task runs in a thread, which Python can't stop, so
    a long IO task should check this now and then and return early.
    """
    job = _current.get()
    return bool(job and job.cancel.is_set())


def current_task_id() -> Optional[str]:
    """The id of the job this code is running for (None outside a job)."""
    job = _current.get()
    return job.task_id if job else None


def run_as(job: Optional[Job], func, args, kwargs):
    """Call func(*args, **kwargs) with `job` as the current job (used in threads and CPU processes)."""
    token = _current.set(job)
    try:
        return func(*args, **kwargs)
    finally:
        _current.reset(token)


# ── Calling task code safely ─────────────────────────────────────────────────

class TaskExited(Exception):
    """A task called sys.exit() / exit() or raised KeyboardInterrupt: the job fails, the worker carries on."""


def _exited(e: BaseException) -> TaskExited:
    if isinstance(e, SystemExit):
        code = e.code if e.code is not None else 0
        return TaskExited(f"The task called sys.exit({code!r}). A task can't stop the worker; the job counts as "
                          f"failed. Raise an exception instead (a library or CLI parser such as argparse or "
                          f"click may have called it).")
    return TaskExited(f"The task raised {type(e).__name__}. A task can't stop the worker; the job counts as "
                      f"failed. Only the worker's own Ctrl+C or SIGTERM stops it.")


def is_async_callable(fn) -> bool:
    """
    True for `async def` functions, also behind decorators (functools.wraps) and
    for objects with `async def __call__`. Such tasks run on the event loop.
    """
    if inspect.iscoroutinefunction(fn):
        return True
    try:
        if inspect.iscoroutinefunction(inspect.unwrap(fn)):
            return True
    except ValueError:            # a __wrapped__ loop
        pass
    call = getattr(fn, "__call__", None)
    return not inspect.isfunction(fn) and not inspect.ismethod(fn) and inspect.iscoroutinefunction(call)


async def _collect_async(gen) -> list:
    return [item async for item in gen]


def _finish_value(value):
    """What a sync call returned, made real: run a coroutine it returned, read out a generator."""
    if inspect.isawaitable(value):
        # A decorator without async support around an `async def`: the call only created the
        # coroutine. Run it here (in this thread or process) instead of storing "<coroutine …>".
        async def wait():
            return await value
        value = asyncio.run(wait())
    if inspect.isasyncgen(value):
        return asyncio.run(_collect_async(value))
    if inspect.isgenerator(value):
        return list(value)            # its code only runs when it's read
    return value


def call_task(job: Optional[Job], func, args, kwargs):
    """
    Run a task's code in a thread or a CPU process. Whatever the code does, the
    worker survives: sys.exit() and KeyboardInterrupt become TaskExited (break test
    E1), and a returned coroutine or generator is run or read (E3).
    """
    token = _current.set(job)
    try:
        return _finish_value(func(*args, **kwargs))
    except (SystemExit, KeyboardInterrupt) as e:
        raise _exited(e) from None
    finally:
        _current.reset(token)


async def acall_task(job: Optional[Job], func, args, kwargs):
    """call_task for the event loop: awaits whatever the call returns."""
    token = _current.set(job)
    try:
        value = func(*args, **kwargs)
        if inspect.isawaitable(value):
            value = await value
        if inspect.isasyncgen(value):
            value = await _collect_async(value)
        elif inspect.isgenerator(value):
            value = list(value)
        return value
    except (SystemExit, KeyboardInterrupt) as e:
        raise _exited(e) from None
    finally:
        _current.reset(token)


class Latest:
    """
    The latest progress of each running job, written by tasks (any thread) and
    taken by the worker's flusher about once a second. Only the newest value
    per job is kept, so a task can report as often as it likes.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._pending: dict[str, tuple[Optional[float], Optional[str], float]] = {}

    def put(self, task_id: str, percent: Optional[float], message: Optional[str]) -> None:
        with self._lock:
            old = self._pending.get(task_id)
            if old is not None:   # a message-only update keeps the last percentage, and the other way round
                percent = old[0] if percent is None else percent
                message = old[1] if message is None else message
            self._pending[task_id] = (percent, message, time.time())

    def take(self) -> dict:
        with self._lock:
            pending, self._pending = self._pending, {}
        return pending

    def drop(self, task_id: str) -> None:
        with self._lock:
            self._pending.pop(task_id, None)
