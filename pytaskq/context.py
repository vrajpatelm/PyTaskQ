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

import contextvars
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


_current: contextvars.ContextVar[Optional[Job]] = contextvars.ContextVar("pytaskq_job", default=None)


def _check(percent, message) -> tuple[Optional[float], Optional[str]]:
    if percent is not None:
        if isinstance(percent, bool) or not isinstance(percent, (int, float)) or percent != percent:
            raise ValueError(f"progress() takes a percentage from 0 to 100, got {percent!r}")
        percent = round(min(100.0, max(0.0, float(percent))), 1)
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
