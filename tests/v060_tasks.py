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


@queue.task(type="io", max_concurrency=1, timeout=0.7, max_retries=1)
def mc_hang(tag=""):
    log("mc_start", tag)
    time.sleep(2)
    log("mc_end", tag)
    return tag


@queue.task(type="io", max_concurrency=1, max_retries=0)
def mc_short(n=0):
    log("mcs_start", n)
    time.sleep(0.1)
    log("mcs_end", n)
    return n


@queue.task(type="io", rate_limit="2/s", max_retries=0)
def rl_task(n=0):
    log("rl_start", n)
    return n


@queue.task(type="io", max_retries=0)
def wf_fetch(url=""):
    log("wf_fetch", url)
    return {"rows": 3, "url": url}


@queue.task(type="io", max_retries=0)
def wf_resize(data, size=64):
    log("wf_resize", size)
    time.sleep(0.2)
    return f"{size}px of {data['rows']} rows"


@queue.task(type="io", max_retries=0)
def wf_publish(images, channel="web"):
    log("wf_publish", channel, len(images))
    return {"published": images, "channel": channel}


@queue.task(type="io", max_retries=1)
def big_error(mb=20):
    raise RuntimeError("E" * (mb * 1_000_000))
