import uuid

import numpy as np
import os
import csv
import random
import io
import time
import urllib.request
import urllib.parse
import urllib.error
import socket
import ipaddress
import hashlib
import hmac
import json
from PIL import Image
from dotenv import load_dotenv

load_dotenv()

class PyTaskQ:
    def __init__(self):
        self.TASKS = {}
        
    def task(self, type="cpu", cron=None):
        """Decorator to register a new task for the distributed workers."""
        def decorator(func):
            task_name = func.__name__
            self.TASKS[task_name] = {"handler": func, "type": type, "cron": cron}

            async def delay(*args, priority="default", on_success_url: str = None):
                from pytaskq.core import redis_client
                import json
                import uuid
                task_id = str(uuid.uuid4())
                task_payload = {
                    "task_name": task_name,
                    "args": list(args),
                    "task_id": task_id,
                    "retry_count": 0,
                    "fence_token": 0,
                    "priority": priority,
                    "on_success_url": on_success_url,
                }
                async with redis_client.r.pipeline(transaction=True) as pipe:
                    pipe.lpush(f"queue:{priority}", json.dumps(task_payload))
                    pipe.incr("stats:pending")
                    await pipe.execute()
                return {"task_id": task_id, "status": "queued"}

            async def schedule(*args, delay_seconds: int = 60, priority="default", on_success_url: str = None):
                from pytaskq.core import redis_client
                import json
                import uuid
                import time
                task_id = str(uuid.uuid4())
                task_payload = {
                    "task_name": task_name,
                    "args": list(args),
                    "task_id": task_id,
                    "retry_count": 0,
                    "fence_token": 0,
                    "priority": priority,
                    "on_success_url": on_success_url,
                }
                execute_at = time.time() + delay_seconds
                async with redis_client.r.pipeline(transaction=True) as pipe:
                    pipe.zadd("delayed_tasks", {json.dumps(task_payload): execute_at})
                    pipe.incr("stats:delayed")
                    await pipe.execute()
                return {"task_id": task_id, "status": "scheduled", "execute_in_seconds": delay_seconds}

            func.delay = delay
            func.schedule = schedule
            return func
        return decorator

# Global SDK instance
queue = PyTaskQ()
# Alias for backward compatibility with worker.py
TASKS = queue.TASKS

# ── SSRF guard ────────────────────────────────────────────────────────────────
# Task handlers fetch user-supplied URLs. Without a guard a tenant could make
# workers request internal services (localhost, 169.254.169.254 cloud
# metadata, RFC1918 ranges, etc.). We resolve the hostname and reject any
# URL whose DNS points at a non-public address, and re-validate every
# redirect hop so http://public.host/ can't bounce us behind the firewall.

class UnsafeURL(Exception):
    """Raised when a user-supplied URL targets a non-public address."""


def _assert_safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise UnsafeURL(f"Only http/https URLs are allowed, got: {parsed.scheme!r}")
    host = parsed.hostname
    if not host:
        raise UnsafeURL("URL has no hostname")
    if host in ("localhost",) or host.endswith((".localhost", ".local", ".internal")):
        raise UnsafeURL(f"Access to internal host {host!r} is not allowed")
    try:
        addr_infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise UnsafeURL(f"Cannot resolve host: {host}")
    for info in addr_infos:
        ip = ipaddress.ip_address(info[4][0])
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local      # 169.254.0.0/16 — cloud metadata lives here
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise UnsafeURL(f"Access to non-public address {ip} is not allowed")


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Re-validates every redirect target before following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _assert_safe_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_SAFE_OPENER = urllib.request.build_opener(_SafeRedirectHandler())


@queue.task(type="cpu")
def matrix_multiply(size: int):
    A = np.random.rand(size, size)
    B = np.random.rand(size, size)
    result = np.dot(A, B)
    # Return shape info only — returning a 1000x1000 float array as JSON
    # would produce a ~8MB response and crash Redis serialization.
    return f"Matrix multiplication complete: {size}x{size} result computed."


@queue.task(type="cpu")
def generate_csv_report(rows: int):
    try:
        start_time = time.time()
        
        # 1. Create an in-memory string buffer
        output = io.StringIO()
        
        # 2. Create a CSV writer attached to the buffer
        writer = csv.writer(output)
        
        # 3. Write the header row
        writer.writerow(["transaction_id", "user_id", "amount", "status"])
        
        # 4. Generate the fake data
        for _ in range(rows):
            transaction_id = str(uuid.uuid4())
            user_id = random.randint(1, 500)
            amount = round(random.uniform(10.0, 1000.0), 2)
            status = random.choice(["Success", "Failed", "Pending"])
            
            writer.writerow([transaction_id, user_id, amount, status])
            
        # 5. Get the full CSV string from the buffer
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
    try:
        start_time = time.time()
        
        # 1. SSRF guard: reject URLs that resolve to private/internal addresses
        _assert_safe_url(image_url)
        
        # 2. Download the image into memory (capped) instead of straight to disk
        request = urllib.request.Request(image_url, headers={"User-Agent": "PyTaskQ-Worker/1.0"})
        with _SAFE_OPENER.open(request, timeout=10) as resp:
            image_bytes = resp.read(10 * 1024 * 1024)  # hard cap: 10 MB
        
        # 3. Open, resize, and encode the image using Pillow
        with Image.open(io.BytesIO(image_bytes)) as img:
            # Convert to RGB in case it's a PNG with transparency
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            
            resized = img.resize((width, height))
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
def url_health_check(url):
    try:
        # SSRF guard: reject URLs that resolve to private/internal addresses
        _assert_safe_url(url)
        time1=time.time()
        resp = _SAFE_OPENER.open(url, timeout=10)
        resp.read(1024)  # drain a little so the socket closes cleanly
        time2 = time.time()
        diff = time2-time1
        return {"url": url, "status_code": resp.status, "response_time_ms": round(diff * 1000)}
    except Exception as e  :
        return {"url": url, "status_code": 0, "error": str(e)}


# ── Webhook Delivery (Retryable) ───────────────────────────────────────────────
# Enqueued as a normal task by the worker after a task succeeds.
# Because it goes through the queue, it gets all retry/DLQ guarantees:
# a flaky network or sleeping server will NOT cause the webhook to silently drop.
@queue.task(type="io")
def _deliver_webhook(url: str, task_id: str, task_name: str, result: str):
    """
    Delivers a signed POST to the user's on_success_url.
    Retried automatically up to 3 times with exponential backoff if the
    target server is unreachable or returns a non-2xx status code.
    """
    _assert_safe_url(url)

    payload = json.dumps({
        "event": "task.success",
        "task_id": task_id,
        "task_name": task_name,
        "result": result,
    }).encode("utf-8")

    # Optional HMAC-SHA256 signing if WEBHOOK_SECRET is set
    secret = os.environ.get("WEBHOOK_SECRET", "")
    signature = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest() if secret else "unsigned"

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "PyTaskQ-Webhook/1.0",
            "X-PyTaskQ-Signature": f"sha256={signature}",
            "X-PyTaskQ-Task-ID": task_id,
        },
        method="POST"
    )
    with _SAFE_OPENER.open(req, timeout=10) as resp:
        status = resp.status
        if status < 200 or status >= 300:
            raise RuntimeError(f"Webhook delivery failed: target returned HTTP {status}")
    return {"delivered_to": url, "status_code": status}
