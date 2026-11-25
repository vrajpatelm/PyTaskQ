"""
pytaskq.pools — where task code runs
=====================================

`type="cpu"` tasks run in separate processes, `type="io"` tasks in threads.

* A job's `timeout` counts from when its function is called, not while it waits for a slot.
* Each CPU process runs one job at a time; a timed-out job's process is killed and replaced.
* A crashed CPU process fails only its own job and is replaced.
* Threads can't be stopped: a timed-out IO job keeps its thread ("stuck") until it returns.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import multiprocessing
import os
import signal
import threading
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Awaitable, Callable, Optional

from pytaskq import context

logger = logging.getLogger("worker")

START_TIMEOUT = 60          # seconds a new CPU process may take to start (imports included)


class TaskTimeoutError(Exception):
    """Raised when a task exceeds its timeout."""


class ProcessCrashed(Exception):
    """The process running a CPU task exited before the task finished."""


class PoolBroken(Exception):
    """The CPU pool couldn't start a replacement process."""


class _RemoteTraceback(Exception):
    def __init__(self, tb: str):
        self.tb = tb

    def __str__(self):
        return self.tb


# ─────────────────────────────────────────────────────────────────────────────
# The CPU process (runs in the child)
# ─────────────────────────────────────────────────────────────────────────────

def _send_error(conn, exc: BaseException, prefix: str = "") -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    message = f"{prefix}{exc}"
    try:
        conn.send(("err", exc if not prefix else None, type(exc).__name__, message, tb))
    except Exception:
        # The exception itself can't be pickled: send its name and text instead.
        conn.send(("err", None, type(exc).__name__, message, tb))


PARENT_CHECK = 1.0      # seconds between a CPU process's checks that its worker is still alive


def _exit_with_parent() -> None:
    """
    Exit when the worker dies (kill -9, OOM). A forked process inherits the parent's ends of the
    pipes, so it never sees its own pipe close; on POSIX its parent id changes when it's orphaned.
    """
    if os.name == "nt":
        return              # spawned processes inherit no pipes: the closed pipe ends them
    parent = os.getppid()

    def watch():
        while True:
            time.sleep(PARENT_CHECK)
            if os.getppid() != parent:
                os._exit(1)
    threading.Thread(target=watch, name="pytaskq-parent-watch", daemon=True).start()


def _child_main(conn, modules: tuple) -> None:
    """One CPU process: import the app, then run one job at a time until told to stop."""
    _exit_with_parent()
    # The worker handles Ctrl+C and SIGTERM itself and decides what happens to
    # running jobs, so a terminal's Ctrl+C mustn't kill jobs half-way.
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.set_wakeup_fd(-1)    # forked from the worker's event loop: don't wake the parent's loop
    except (ValueError, OSError, AttributeError):
        pass
    for name in modules:
        try:
            importlib.import_module(name)
        except Exception as e:   # the job itself will report a useful error
            print(f"[pytaskq cpu process {os.getpid()}] could not import {name}: {e}", flush=True)
    conn.send(("ready", os.getpid()))
    while True:
        try:
            job = conn.recv()
        except (EOFError, OSError, KeyboardInterrupt):
            return
        except Exception as e:     # the job's function couldn't be unpickled (module missing, …)
            _send_error(conn, e)
            continue
        if job is None:
            return
        func, args, kwargs, *rest = job
        try:
            value = context.call_task(_child_job(conn, rest[0] if rest else None), func, args, kwargs)
        except BaseException as e:     # anything else that isn't an Exception: still the job's failure, as text
            if not isinstance(e, Exception):
                e = context.TaskExited(f"The task raised {type(e).__name__}: {e}")
            _send_error(conn, e)
            continue
        try:
            conn.send(("ok", value))
        except Exception as e:
            _send_error(conn, e, prefix="The task finished, but its return value couldn't be sent back: ")


PROGRESS_EVERY = 0.2     # seconds between two progress messages from a CPU process (newer ones replace older)


def _child_job(conn, task_id):
    """The job context inside a CPU process: progress() goes to the worker through the pipe."""
    if not task_id:
        return None
    last = [0.0]

    def report(_, percent, message):
        now = time.monotonic()
        if now - last[0] >= PROGRESS_EVERY:
            last[0] = now
            try:
                conn.send(("progress", percent, message))
            except Exception:
                pass
    return context.Job(task_id, report)


def _raise_child_error(msg) -> None:
    _, exc, type_name, message, tb = msg
    if exc is None or not isinstance(exc, Exception):     # never re-raise SystemExit & co in the worker
        exc = type(type_name, (Exception,), {})(message)
    exc.__cause__ = _RemoteTraceback(tb)
    raise exc


def describe_exit(code: Optional[int]) -> str:
    if code is None:
        return "stopped responding"
    if code < 0:
        try:
            name = signal.Signals(-code).name
        except (ValueError, AttributeError):
            name = f"signal {-code}"
        hint = " (often the out-of-memory killer)" if name == "SIGKILL" else ""
        return f"was killed by {name}{hint}"
    return f"exited with code {code} (a crash in a C extension, os._exit() or running out of memory)"


class _Child:
    """A CPU process and the parent's end of its pipe. Used by one job at a time."""

    def __init__(self, process, conn):
        self.process = process
        self.conn = conn
        self.pid = process.pid

    def call(self, func, args, kwargs, job=None):
        """Runs in a helper thread: send the job, wait for the answer. Raises ProcessCrashed if the process dies."""
        try:
            self.conn.send((func, tuple(args), dict(kwargs), job.task_id if job else None))
        except (BrokenPipeError, EOFError, ConnectionError) as e:
            raise ProcessCrashed(f"The CPU process {describe_exit(self.exitcode())} before the task started ({e}).")
        while True:
            try:
                if self.conn.poll(0.5):
                    msg = self.conn.recv()
                    if isinstance(msg, tuple) and msg and msg[0] == "progress":
                        if job is not None:
                            job.report(job.task_id, msg[1], msg[2])
                        continue
                    break
            except (EOFError, OSError):
                raise ProcessCrashed(f"The process running this task {describe_exit(self.exitcode())}.") from None
            except Exception as e:      # the reply arrived but can't be unpickled here
                raise RuntimeError(f"The task's process sent back something that can't be read: "
                                   f"{type(e).__name__}: {e}") from None
            if not self.process.is_alive():
                raise ProcessCrashed(f"The process running this task {describe_exit(self.exitcode())}.")
        if msg[0] == "ok":
            return msg[1]
        _raise_child_error(msg)

    def exitcode(self) -> Optional[int]:
        try:
            self.process.join(1)
        except Exception:
            pass
        return self.process.exitcode

    def kill(self) -> None:
        try:
            if self.process.is_alive():
                self.process.kill()
            self.process.join(5)
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass


class CpuPool:
    """
    A fixed number of processes for type="cpu" tasks, each running one job at a time.

    Unlike ProcessPoolExecutor, it knows which process runs which job, so a job that
    times out or crashes costs only its own process, which is replaced.
    """

    def __init__(self, size: int, modules: tuple = (), mp_context=None):
        self.size = max(1, int(size))
        self.modules = tuple(m for m in modules if m and m != "__main__")
        self._ctx = mp_context or multiprocessing.get_context()
        self._spawn_lock = threading.Lock()
        self._threads = ThreadPoolExecutor(max_workers=self.size + 2, thread_name_prefix="pytaskq-cpu")
        self._idle: Optional[asyncio.Queue] = None
        self._children: set[_Child] = set()
        self._restarting: set[asyncio.Task] = set()
        self._closed = False
        self.draining = False                   # shutting down: don't replace stopped processes
        self.restarts = 0                       # processes replaced after a timeout or crash
        self.broken = asyncio.Event()           # set when a replacement couldn't be started
        self.broken_reason = ""

    # ── processes ────────────────────────────────────────────────────────────
    def _spawn(self) -> _Child:
        # One at a time: a process started (forked) while another one's pipe is
        # still open in this process would inherit that pipe and hide its death.
        with self._spawn_lock:
            if self._closed:
                raise PoolBroken("The CPU pool is closed.")
            parent_conn, child_conn = self._ctx.Pipe(duplex=True)
            process = self._ctx.Process(target=_child_main, args=(child_conn, self.modules),
                                        name="pytaskq-cpu", daemon=False)
            process.start()
            child_conn.close()
            child = _Child(process, parent_conn)
            self._children.add(child)          # close() sees it even while it's starting
        try:
            ready = parent_conn.poll(START_TIMEOUT) and parent_conn.recv()
        except (EOFError, OSError):
            ready = None
        if not ready or ready[0] != "ready":
            self._children.discard(child)
            child.kill()
            raise PoolBroken(f"A CPU process didn't start: it {describe_exit(process.exitcode)}.")
        return child

    async def start(self) -> None:
        """Start every process now, so the first jobs don't pay for start-up (or imports) inside their timeout."""
        loop = asyncio.get_running_loop()
        self._idle = asyncio.Queue()
        children = await asyncio.gather(*(loop.run_in_executor(self._threads, self._spawn)
                                          for _ in range(self.size)), return_exceptions=True)
        errors = [c for c in children if isinstance(c, BaseException)]
        for c in children:
            if isinstance(c, _Child):
                self._idle.put_nowait(c)
        if errors:
            await self.close(grace=0)
            raise PoolBroken(str(errors[0]))
        import atexit
        atexit.register(self.kill_all_now)     # never leave a process behind, whatever ends the worker

    async def _replace(self, child: _Child, pending=None) -> None:
        """Kill `child` (if still alive) and put a fresh process in its slot."""
        loop = asyncio.get_running_loop()
        self._children.discard(child)
        await loop.run_in_executor(self._threads, child.kill)
        if pending is not None:
            try:
                await asyncio.wait_for(asyncio.shield(pending), 10)   # its helper thread notices and returns
            except BaseException:
                pass
        if self._closed or self.draining:
            return
        delay = 1.0
        for attempt in range(1, 4):
            try:
                fresh = await loop.run_in_executor(self._threads, self._spawn)
            except Exception as e:
                if self._closed:
                    return
                logger.error(f"[CPU pool] Couldn't start a replacement process (attempt {attempt}/3): {e}")
                await asyncio.sleep(delay)
                delay *= 3
                continue
            if self._closed:
                self._children.discard(fresh)
                await loop.run_in_executor(self._threads, fresh.kill)
                return
            self.restarts += 1
            assert self._idle is not None            # set by start()
            self._idle.put_nowait(fresh)
            return
        self.broken_reason = "The worker couldn't start new CPU processes."
        self.broken.set()

    def _replace_soon(self, child: _Child, pending=None) -> None:
        task = asyncio.ensure_future(self._replace(child, pending))
        self._restarting.add(task)
        task.add_done_callback(self._restarting.discard)

    # ── running a job ────────────────────────────────────────────────────────
    async def execute(self, func: Callable, args, kwargs, timeout: float,
                      started: Callable[[], Awaitable[None]], job=None) -> Any:
        """Wait for a free process, call `started()`, then run the job with its timeout."""
        if self._idle is None:
            raise RuntimeError("CpuPool.start() wasn't called")
        if self.broken.is_set():
            raise PoolBroken(self.broken_reason)
        child = await self._idle.get()
        try:
            await started()
        except BaseException:
            self._idle.put_nowait(child)
            raise
        loop = asyncio.get_running_loop()
        pending = loop.run_in_executor(self._threads, child.call, func, args, kwargs, job)
        try:
            if timeout and timeout > 0:
                result = await asyncio.wait_for(asyncio.shield(pending), timeout)
            else:
                result = await asyncio.shield(pending)
        except asyncio.TimeoutError:
            if pending.done() and not pending.cancelled():
                # Not our timeout: the task raised TimeoutError itself (or finished right at the limit).
                self._idle.put_nowait(child)
                return pending.result()
            # The function is still running in its process: stop it, start a new one.
            self._replace_soon(child, pending)
            raise TaskTimeoutError(f"Task exceeded {timeout:g}s limit. Its process was stopped.") from None
        except asyncio.CancelledError:
            self._replace_soon(child, pending)    # worker shutting down after its grace period
            raise
        except ProcessCrashed:
            logger.error(f"[CPU pool] Process {child.pid} died while running a task; starting a new one.")
            self._replace_soon(child)
            raise
        except BaseException:
            self._idle.put_nowait(child)          # the task raised: its process is fine
            raise
        self._idle.put_nowait(child)
        return result

    # ── shutting down ────────────────────────────────────────────────────────
    async def close(self, grace: float = 5) -> None:
        """Stop every process: ask idle ones to exit, kill whatever is left after `grace` seconds."""
        loop = asyncio.get_running_loop()
        with self._spawn_lock:             # no process is half-started from here on
            self._closed = True
            children = list(self._children)
            self._children.clear()
        for task in list(self._restarting):
            task.cancel()

        def stop_all():
            for c in children:
                try:
                    c.conn.send(None)
                except Exception:
                    pass
            deadline = time.monotonic() + max(0.0, grace)
            for c in children:
                c.process.join(max(0.0, deadline - time.monotonic()))
            for c in children:
                c.kill()

        await loop.run_in_executor(None, stop_all)
        self._threads.shutdown(wait=False, cancel_futures=True)

    def kill_all_now(self) -> None:
        """Last resort at exit (no event loop): kill every process."""
        with self._spawn_lock:
            self._closed = True
        for c in list(self._children):
            c.kill()
        self._children.clear()


# ─────────────────────────────────────────────────────────────────────────────
# IO threads
# ─────────────────────────────────────────────────────────────────────────────

def _call(job, func, args, kwargs):
    return context.call_task(job, func, args, kwargs)


class IoPool:
    """
    Threads for type="io" tasks (and for type="cpu" when there's no process pool).

    A job holds its slot until its function returns, even after its timeout (Python
    can't stop a thread); `stuck` counts those threads.
    """

    def __init__(self, size: int, executor: Optional[ThreadPoolExecutor] = None):
        self.size = max(1, int(size))
        self.executor = executor or ThreadPoolExecutor(max_workers=self.size, thread_name_prefix="pytaskq-io")
        self._free: Optional[asyncio.Semaphore] = None
        self.stuck: dict[int, tuple[str, str, float]] = {}    # id → (task id, task name, since)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _slots(self) -> asyncio.Semaphore:
        if self._free is None:
            self._free = asyncio.Semaphore(self.size)
            self._loop = asyncio.get_running_loop()
        return self._free

    @property
    def all_stuck(self) -> bool:
        return len(self.stuck) >= self.size

    def running_threads(self) -> int:
        return len(self.stuck)

    async def execute(self, func: Callable, args, kwargs, timeout: float,
                      started: Callable[[], Awaitable[None]], label: tuple[str, str] = ("", ""), job=None) -> Any:
        slots = self._slots()
        await slots.acquire()
        keep_slot = False
        try:
            await started()
            if isinstance(self.executor, ProcessPoolExecutor):
                job = None        # can't cross into another process (an embedder's own process pool)
            cf = self.executor.submit(_call, job, func, args, kwargs)
            fut = asyncio.wrap_future(cf)
            try:
                if timeout and timeout > 0:
                    return await asyncio.wait_for(asyncio.shield(fut), timeout)
                return await asyncio.shield(fut)
            except (asyncio.TimeoutError, asyncio.CancelledError) as e:
                if isinstance(e, asyncio.TimeoutError) and fut.done() and not fut.cancelled():
                    return fut.result()    # the task's own TimeoutError (or it finished right at the limit)
                if not cf.done():
                    keep_slot = True
                    self._hold_until_done(cf, fut, label)
                    if job is not None:
                        job.left_running = cf     # its max_concurrency place is kept until it returns
                if isinstance(e, asyncio.TimeoutError):
                    raise TaskTimeoutError(
                        f"Task exceeded {timeout:g}s limit. Python can't stop a running thread, so its code may "
                        f"still be running; give network calls in IO tasks their own timeouts."
                    ) from None
                raise
        finally:
            if not keep_slot:
                slots.release()

    def _hold_until_done(self, cf, fut, label) -> None:
        key = id(cf)
        self.stuck[key] = (label[0], label[1], time.time())
        loop, free = self._loop, self._free
        assert loop is not None and free is not None      # set by _slots() before any job runs
        fut.add_done_callback(lambda f: f.cancelled() or f.exception())   # don't warn about an unread error

        def finished(_):
            def release():
                if self.stuck.pop(key, None) is not None:
                    free.release()
            try:
                loop.call_soon_threadsafe(release)
            except RuntimeError:
                pass      # the loop is closed: the worker has stopped
        cf.add_done_callback(finished)

    def close(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)
