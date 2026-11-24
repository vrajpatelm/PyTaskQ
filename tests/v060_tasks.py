"""Tasks for test_v060.py: module level, so CPU processes and real worker processes can import them."""

import functools
import os
import sys
import time

from pytaskq import PyTaskQ

queue = PyTaskQ()
RUNLOG = os.environ.get("RUNLOG", "")


def log(*parts):
    if RUNLOG:
        with open(RUNLOG, "a") as f:
            f.write(" ".join(map(str, parts)) + f" {time.time():.3f} {os.getpid()}\n")


def logged(fn):
    """An ordinary decorator that knows nothing about async."""
    @functools.wraps(fn)
    def wrapper(*a, **k):
        return fn(*a, **k)
    return wrapper


@queue.task(type="io", max_retries=0)
def io_exit(code=3):
    sys.exit(code)


@queue.task(type="io", max_retries=0)
def io_kbint():
    raise KeyboardInterrupt


@queue.task(max_retries=0)
async def async_exit(code=3):
    sys.exit(code)


@queue.task(max_retries=0)
async def async_kbint():
    raise KeyboardInterrupt


@queue.task(type="cpu", max_retries=0)
def cpu_exit(code=3):
    sys.exit(code)


@queue.task(type="io", max_retries=0)
def echo(x=0, tag=""):
    log("echo", tag, x)
    return x


@queue.task(type="io", max_retries=0)
def slow_io(sec=2.0, tag=""):
    log("slow_start", tag)
    time.sleep(sec)
    log("slow_end", tag)
    return tag


@queue.task(type="io", max_retries=0)
@logged
async def wrapped_async(tag=""):
    log("wrapped_async_ran", tag)
    return "did work"


@queue.task(max_retries=0)
@logged
async def wrapped_async_default(tag=""):
    log("wrapped_async_default_ran", tag)
    return "did work too"


def cpu_double(x):
    return x * 2


def cpu_exit_fn():
    sys.exit(4)


def cpu_wrapped_coroutine():
    async def inner():
        return "awaited in the process"
    return inner()


def cpu_generator(n):
    for i in range(n):
        yield i * i
