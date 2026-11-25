"""
pytaskq.task_registery — built-in demo tasks
=============================================

These example tasks power the default dashboard demo and the Docker worker
(`pytaskq worker --app pytaskq.task_registery:queue`). Your own project should
create its own `PyTaskQ()` instance — see example_app.py.

Also re-exports, for compatibility:
    PyTaskQ, queue, TASKS, UnsafeURL, _assert_safe_url, _SafeRedirectHandler, _SAFE_OPENER
"""

import csv
import io
import random
import time
import urllib.request
import uuid

from pytaskq.app import PyTaskQ, _deliver_webhook  # noqa: F401  (re-exported)
from pytaskq.net import (  # noqa: F401  (re-exported for backwards compatibility)
    SAFE_OPENER,
    UnsafeURL,
    assert_safe_url,
    _assert_safe_url,
    _SafeRedirectHandler,
    _SAFE_OPENER,
)

queue = PyTaskQ("demo")
# Kept for older worker code
TASKS = queue.TASKS


@queue.task(type="cpu")
def matrix_multiply(size: int):
    """Multiply two random N x N matrices (CPU-heavy, needs numpy)."""
    import numpy as np
    if not 1 <= size <= 1000:
        raise ValueError("size must be between 1 and 1000")
    A = np.random.rand(size, size)
    B = np.random.rand(size, size)
    result = np.dot(A, B)
    # Return a summary: the matrix itself would be megabytes of JSON.
    return f"Matrix multiplication complete: {size}x{size} result computed."


@queue.task(type="cpu")
def generate_csv_report(rows: int):
    """Generate an in-memory CSV report with fake transactions."""
    try:
        start_time = time.time()

        output = io.StringIO()

        writer = csv.writer(output)

        writer.writerow(["transaction_id", "user_id", "amount", "status"])

        for _ in range(rows):
            transaction_id = str(uuid.uuid4())
            user_id = random.randint(1, 500)
            amount = round(random.uniform(10.0, 1000.0), 2)
            status = random.choice(["Success", "Failed", "Pending"])

            writer.writerow([transaction_id, user_id, amount, status])

        csv_string = output.getvalue()
        output.close()

        end_time = time.time()

        return {
            "status": "success",
            "rows_generated": rows,
            "bytes_size": len(csv_string),
            "time_taken_ms": round((end_time - start_time) * 1000)
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}



@queue.task(type="cpu")
def resize_image(image_url: str, width: int, height: int):
    """Download an image (SSRF-guarded, 10 MB cap) and resize it with Pillow."""
    from PIL import Image
    try:
        start_time = time.time()

        # SSRF guard: reject private/internal addresses
        assert_safe_url(image_url)

        request = urllib.request.Request(image_url, headers={"User-Agent": "PyTaskQ-Worker/1.0"})
        with SAFE_OPENER.open(request, timeout=10) as resp:
            image_bytes = resp.read(10 * 1024 * 1024)  # hard cap: 10 MB

        with Image.open(io.BytesIO(image_bytes)) as img:
            # JPEG has no transparency
            rgb = img.convert("RGB") if img.mode in ("RGBA", "P") else img
            resized = rgb.resize((width, height))
            buffer = io.BytesIO()
            resized.save(buffer, format="JPEG", quality=85)

        end_time = time.time()

        return {
            "status": "success",
            "bytes_processed": len(image_bytes),
            "original_url": image_url,
            "dimensions": f"{width}x{height}",
            "time_taken_ms": round((end_time - start_time) * 1000)
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


@queue.task(type="io")
def url_health_check(url: str):
    """Check that a public URL responds and measure its latency."""
    try:
        # SSRF guard: reject private/internal addresses
        assert_safe_url(url)
        time1=time.time()
        resp = SAFE_OPENER.open(url, timeout=10)
        resp.read(1024)  # drain a little so the socket closes cleanly
        time2 = time.time()
        diff = time2-time1
        return {"url": url, "status_code": resp.status, "response_time_ms": round(diff * 1000)}
    except Exception as e  :
        return {"url": url, "status_code": 0, "error": str(e)}
