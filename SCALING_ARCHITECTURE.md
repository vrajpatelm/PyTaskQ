# Scaling PyTaskQ to 1,000+ Users

This document outlines the architectural shift required to move PyTaskQ from a "single-server internal tool" to a high-traffic "Multi-Tenant Task Queue" capable of handling 1,000+ users simultaneously.

## 1. ✅ Implement Task Priority Queues (DONE)

**Approach chosen: Multiple Lists**

Instead of a single `task_queue`, the system now uses three separate Redis Lists consumed in strict priority order:
- `queue:high` — Urgent tasks (emails, webhooks, crash-recovered tasks)
- `queue:default` — Standard user tasks
- `queue:low` — Background / batch work

The worker polls these in order using `RPOPLPUSH`, ensuring `queue:high` is always drained before `queue:default` is touched. If all queues are empty, the worker sleeps for 1 second before polling again.

## 2. Per-Tenant Backpressure
*Currently, `QUEUE_CAPACITY` is a global limit across all three priority queues. Two spammy users can fill this instantly, causing 503 "Server Busy" errors for the other 998 users.*

**The Solution:**
- Remove the global queue length check in `app.py`.
- Check the user's isolated pending limit: `get("stats:pending:{client_ip}")`.
- If a single user has >50 pending tasks, they get a 429 error. Everyone else can continue using the queue freely. This guarantees fairness.

## 3. Generous Rate Limits (Token Bucket)
*Currently, users are hard-limited to 10 requests per minute.*

**The Solution:**
- Because Per-Tenant Backpressure (above) protects the server, the strict API rate limit is no longer necessary.
- Increase the rate limit to `100 req / minute`. Users will experience a fast, free-flowing API. If they abuse it, they simply hit their personal queue cap.

## 4. Horizontal Worker Scaling
*A single Python instance with 4 CPU process workers will choke on 1,000 users.*

**The Solution:**
- Because Redis acts as a centralized broker and handles atomic locking (via `RPOPLPUSH`), you can run as many workers as you want without changing any Python code.
- In `docker-compose.yml`, update the worker service to spin up multiple replicas:
  ```yaml
  worker:
    build: .
    deploy:
      replicas: 5 # Spins up 5 worker instances (20 CPU processes total)
  ```

## 5. API Key Migration
*Currently, tenants are tracked by IP address. Behind NATs (like college Wi-Fi) or proxies, hundreds of users look like they have the exact same IP, causing them to share metrics and limits.*

**The Solution:**
- Require users to register for an API Key (UUID).
- Pass it in the header: `Authorization: Bearer <API_KEY>`.
- Replace all `client_ip` state tracking with `api_key` state tracking.
