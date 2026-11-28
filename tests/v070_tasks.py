"""Tasks for tests/test_v070.py's real-worker tests. Each run is logged to $RUNLOG."""
import asyncio
import os
import time

from pytaskq import PyTaskQ

queue = PyTaskQ()


def _log(*parts):
    path = os.environ.get("RUNLOG")
    if path:
        with open(path, "a") as f:
            f.write(" ".join(map(str, parts)) + f" {time.time():.3f} {os.getpid()}\n")


@queue.task(type="io", max_retries=0)
def echo(x=0, tag=""):
    _log("echo", tag, x)
    return x


@queue.task(type="io", max_retries=0, timeout=0)
def slow(sec=5.0, tag=""):
    _log("slow_start", tag)
    time.sleep(sec)
    _log("slow_end", tag)
    return tag


@queue.task(type="io", max_retries=5)
def hard_exit(tag=""):
    """Kills its own process, like a segfault in a C extension or the out-of-memory killer."""
    _log("hard_exit", tag)
    os._exit(1)


@queue.task(max_retries=0, timeout=0)
async def stubborn_forever(tag=""):
    """Catches every cancellation and keeps going (a bare except, a retry helper)."""
    _log("stubborn_start", tag)
    while True:
        try:
            await asyncio.sleep(0.2)
        except BaseException:
            _log("stubborn_swallowed", tag)


class Weird(Exception):
    """Like many client-library errors: pickles, but can't be rebuilt from its args."""

    def __init__(self, a, b):
        super().__init__(a)
        self.b = b


def raise_weird():
    raise Weird("x", "y")


@queue.task(type="io", max_retries=0, rate_limit="10/s")
def paced(tag=""):
    _log("paced", tag)
