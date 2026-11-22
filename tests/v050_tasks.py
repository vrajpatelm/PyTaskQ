"""Task functions for test_v050.py (module level, so CPU processes can import them)."""

import time

from pytaskq import cancelled, current_task_id, progress


def steps(n=3, pause=0.25):
    for i in range(n):
        progress(100 * (i + 1) / n, f"step {i + 1} of {n}")
        time.sleep(pause)
    return current_task_id()


def spin(seconds=30):
    """Busy for a long time, never looks at cancelled()."""
    end = time.time() + seconds
    while time.time() < end:
        time.sleep(0.05)
    return "finished"


def polite(seconds=30):
    """Long IO-style work that stops early when cancelled."""
    end = time.time() + seconds
    while time.time() < end:
        if cancelled():
            return "stopped early"
        time.sleep(0.05)
    return "finished"
