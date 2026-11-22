"""Task functions for the break-test regression tests (module level, so CPU processes can import them)."""

import os
import time


def hang(n=0):
    while True:
        time.sleep(0.2)


def quick(seconds=0.3, value=0):
    time.sleep(seconds)
    return value


def crash():
    os._exit(1)


def double(n=0):
    return n * 2


def pid():
    return os.getpid()


def raises_value_error():
    raise ValueError("bad input")


def raises_timeout_error():
    raise TimeoutError("the remote server timed out")


class Unpicklable(Exception):
    def __init__(self, a, b):           # can't be rebuilt from its args alone
        super().__init__(f"{a}/{b}")
        self.lock = __import__("threading").Lock()


def raises_unpicklable():
    raise Unpicklable(1, 2)


def returns_unpicklable():
    return lambda: 1


def calls_exit():
    raise SystemExit(3)
