# PyTaskQ — API & Dashboard Reference

While PyTaskQ primarily operates over Redis, it optionally ships with a lightweight FastAPI server that hosts a beautiful Real-Time React Dashboard and exposes REST endpoints for queue observability and management.

By default, the server runs on `http://localhost:8000`.

---

## 1. Starting the API & Dashboard

```bash
pytaskq dashboard --app your_module:queue      # --app is optional; it lists your tasks before a worker starts
# or: uvicorn pytaskq.main:app --reload
```
*Note: Make sure your `Redis` instance is running, as the API relies entirely on reading live Redis data.*

---

## 2. Queue Metrics Endpoint

Returns live cluster-wide queue and worker statistics.

*   **URL**: `/metrics`
*   **Method**: `GET`
*   **Response (200 OK)**:
    ```json
    {
      "pending": 0,
      "processing": 0,
      "delayed": 0,
      "dlq": 0,
      "completed_total": 0,
      "failed_total": 0,
      "queue_high": 0,
      "queue_default": 0,
      "queue_low": 0,
      "queue_total": 0,
      "queue_capacity": 500,
      "queues": [{"name": "default", "high": 0, "default": 0, "low": 0, "total": 0, "workers": 2},
                 {"name": "images", "high": 0, "default": 4, "low": 0, "total": 4, "workers": 1}],
      "fullest_queue": {"name": "images", "total": 4},
      "worker_count": 3
    }
    ```

`pending`, `processing`, `delayed` and `dlq` are counted from Redis on every call. `queue_high`/`queue_default`/`queue_low`/`queue_total` add up every queue. `queue_capacity` applies to each queue separately, and `queues` breaks the numbers down per queue, with how many live workers serve each. `completed_total` and `failed_total` are all-time totals.

---

## 3. Task Lookup Endpoint

Fetch the current execution state, result output, or error stacktrace for a specific task.

*   **URL**: `/task/{task_id}`
*   **Method**: `GET`
*   **URL Parameters**:
    *   `task_id` (string, required): The UUID of the task.
*   **Response (200 OK)**:
    *   *If task has completed successfully*:
        ```json
        {
          "result": {
            "task_id": "42ba7f52-8703-455f-8fe0-2b28c8de1e2b",
            "status": "Success",
            "result": "Matrix multiplication complete: 100x100 result computed."
          }
        }
        ```
    *   *If task is scheduled for a retry after failure*:
        ```json
        {
          "result": {
            "task_id": "4b977712-40eb-485a-8b83-b6c8ab0be928",
            "status": "RetryScheduled",
            "retry_count": "1",
            "error": "Error: Connection refused..."
          }
        }
        ```
*   **Errors**: `404` — task not found (or has expired past 24h).

---

## 4. Dead Letter Queue (DLQ) Management

Tasks that exceed their maximum retry count are routed to the Dead Letter Queue (`dlq`).

### View DLQ Tasks
Retrieve all tasks currently stored in the Dead Letter Queue.

*   **URL**: `/dlq`
*   **Method**: `GET`
*   **Response (200 OK)**:
    ```json
    {
      "tasks": [
        {
          "task_id": "52ba8f52...",
          "task_name": "resize_image",
          "args": ["s3://bucket/bad-image.jpg"],
          "retry_count": 3,
          "error": "File not found"
        }
      ]
    }
    ```

### Replay Specific DLQ Task
Atomically move a failed task back to the active queue (resets retry count to 0).

*   **URL**: `/dlq/replay/{task_id}`
*   **Method**: `POST`

### Replay ALL DLQ Tasks
Atomically sweeps the entire Dead Letter Queue back into active execution. Perfect for bulk recovery after a database outage.

*   **URL**: `/dlq/retry_all`
*   **Method**: `POST`

### Purge DLQ
Delete all failed tasks permanently.

*   **URL**: `/dlq/purge_all`
*   **Method**: `POST`


---

## Jobs API (v0.2)

### Enqueue — `POST /task/enqueue`

```json
{
  "task_name": "send_receipt",
  "args": [1001],
  "kwargs": {"email": "ada@example.com"},
  "priority": "high",          // optional; defaults to the task's own priority
  "queue": "emails",           // optional; defaults to the task's own queue (see docs/QUEUES.md)
  "delay_seconds": 0,          // optional; > 0 schedules the job
  "on_success_url": null       // optional webhook
}
```

Response: `{"task_id": "…", "status": "queued"}` (or `"scheduled"` with `execute_in_seconds`).

Errors:
* `403` — the task name starts with `_`: those are PyTaskQ's internal tasks (such as webhook delivery) and can't be queued through the API.
* `404` — the task name isn't advertised by any running worker. The message lists the available tasks.
* `413` — the job is over `MAX_PAYLOAD_BYTES` (1 MB) once encoded. Put large data in storage and pass its location.
* `422` — invalid body: a task name over 200 characters, an unknown priority, an invalid queue name, a delay outside 0 to 1 year, `NaN`/`Infinity` in the arguments, a malformed `Idempotency-Key`, or a key already used for a different job. Also **arguments the task doesn't accept** (a missing required argument, an unknown keyword, too many positional ones), checked against the parameters the workers advertise: `"Wrong arguments for task 'send_receipt': missing a required argument: 'order_id'"`. Long rejected values are shortened in the reply.
* `429` — the job's queue already holds `QUEUE_CAPACITY` jobs (each queue has its own limit). The check and the push are one step, so the limit is exact under any load. Scheduled jobs (`delay_seconds` > 0) are refused too while their queue is full. Wait `Retry-After` seconds and try again.
* `503` — the server couldn't reach Redis in time. Also carries `Retry-After`.

`POST /task/schedule?delay_seconds=60` still works and takes the same body.

Only the API checks `QUEUE_CAPACITY`. Jobs queued by your app's code (`.delay()`), cron, retries and dead-letter replays are never refused, since refusing them would lose work your app already decided to do.

#### Safe retries with `Idempotency-Key`

A client that times out, or gets a 429 or 503, can't tell whether its job was queued. Send any unique string (a UUID works well) in an `Idempotency-Key` header, and resend the exact same request when in doubt:

```bash
curl -X POST https://jobs.example.com/task/enqueue \
  -H "Authorization: Bearer ptq_…" -H "Idempotency-Key: order-1042-receipt" \
  -H "Content-Type: application/json" -d '{"task_name": "send_receipt", "args": [1042]}'
```

* The first request queues the job. Repeats within `IDEMPOTENCY_TTL` (a day) return the **same** response plus `"duplicate": true`, and queue nothing.
* Repeats are answered even while the queue is full, since that job was already accepted.
* Reusing a key for a *different* job (other task, arguments, priority or delay) is a `422`.
* Keys are 1 to 255 printable characters without spaces, and shared by everyone using this server.

The SDK takes the same option: `await send_receipt.delay(1042, idempotency_key="order-1042-receipt")`.

### Job status — `GET /task/{task_id}`

A job is visible from the moment it is queued:

```json
{
  "result": {
    "task_id": "7e1cb51f-…",
    "task_name": "send_receipt",
    "status": "Success",              // Queued | Scheduled | Processing | RetryScheduled | Success | Failed | DeadLetter | Cancelled
    "priority": "high",
    "args": [1001],
    "kwargs": {"email": "ada@example.com"},
    "created_at": 1795412000.12,
    "started_at": 1795412000.20,
    "finished_at": 1795412000.41,
    "duration_ms": 203.0,
    "retry_count": 0,
    "worker_id": "web-1-1234-a1b2c3",
    "result": "{\"order_id\": 1001, \"sent_to\": \"ada@example.com\"}",
    "error": ""
  }
}
```

Non-string results are stored as JSON. Results over `MAX_RESULT_BYTES` (1 MB) aren't stored: the job is still `Success`, `result` is empty and `result_dropped` explains why. The record also has `queue`, and a `note` while a job waits for a worker that has its task.

### Cancel — `POST /task/{task_id}/cancel`

Works for `Queued`, `Scheduled` and `RetryScheduled` jobs. Returns `409` for jobs that are running or finished, and `404` for unknown ones.

### History — `GET /tasks`

Query: `status`, `task_name`, `queue`, `limit` (1–500). Newest first, drawn from the last `HISTORY_LIMIT` jobs.

### Registered tasks — `GET /tasks/registered`

What running workers (or `pytaskq dashboard --app …`) advertise, including each parameter. Workers running different apps are all listed; a task drops out about 5 minutes after nothing advertises it any more.

```json
{"tasks": [{"name": "send_receipt", "type": "io", "queue": "default", "priority": "high", "max_retries": 3,
            "timeout": 300, "cron": null, "tz": null, "doc": "Email a receipt.",
            "params": [{"name": "order_id", "required": true, "annotation": "int"},
                       {"name": "email", "required": false, "default": "customer@example.com", "annotation": "str"}]}]}
```

### Workers — `GET /system/workers`

Each worker reports `hostname`, `pid`, `app`, `queues` (the queues it serves), `active`, `processed`, `failed`, `cpu_workers`, `io_workers`, `uptime_seconds`, `last_heartbeat_seconds_ago`, `stuck_io` (IO threads still running timed-out tasks), `cpu_restarts` (CPU processes replaced after a timeout or crash), `loop_blocked_seconds` (how long an async task has blocked it right now, 0 if not) and `status` (`healthy` / `degraded` / `dead`).

### Dead letters

Each `GET /dlq` entry includes `error` (the last exception) and `failed_at`. Entries with `"invalid": true` (task name `"(invalid job)"`) weren't valid jobs at all, such as text pushed onto a queue by hand; they carry `raw` (the first 64 KB of what was sent) and `found_in`, and can't be replayed (`409`), only deleted. At most `DLQ_LIMIT` (10,000) dead letters are kept; beyond that the oldest are dropped. `replay` and `retry_all` put jobs back on their **original priority queue** with a valid fence token. Each dead letter can only be taken once: if two people replay (or delete) the same job at the same moment, the second gets `409` and the job runs once.

---

## Authentication (v0.3)

Every route except `/health`, `/` (the dashboard page) and the public `/auth` routes requires a caller:

| Caller | How | Permissions |
|---|---|---|
| Dashboard | `pytaskq_session` cookie, set by `/auth/login`, `/auth/setup` or accepting an invite | The user's role |
| Script or service | `Authorization: Bearer ptq_…` (created under *Settings → API tokens*) | The token owner's role |
| Automation | `Authorization: Bearer <MASTER_KEY>` (only if set, ≥ 16 characters) | Admin |

Roles: **viewer** can use every `GET` route and the live WebSocket. **operator** can also enqueue, schedule and cancel jobs and replay or delete dead letters. **admin** can also use the `/auth/users` and `/auth/invites` management routes.

Errors: `401` means not signed in, or the token is invalid or revoked. `403` means the role is too low, or a cookie-authenticated write came from another website.

With `PYTASKQ_AUTH=off`, every request is treated as an admin called `local`.

### Public

| Method | Route | Body | Notes |
|---|---|---|---|
| `GET` | `/auth/status` | — | `{"auth_enabled", "setup_required", "user"}` |
| `POST` | `/auth/setup` | `{"setup_code", "username", "password"}` | Only while no account exists; signs you in |
| `POST` | `/auth/login` | `{"username", "password"}` | After 10 failed attempts per user (or 30 per IP) in 15 minutes, further attempts get `429`. Attempts are counted before the password is checked, so parallel guesses can't exceed the limit. The IP is the connection's (or the one your reverse proxy reports, see `FORWARDED_ALLOW_IPS`), never a client-sent `X-Forwarded-For`. |
| `GET` | `/auth/invites/{token}` | — | `{"role", "created_by", "expires_at"}` |
| `POST` | `/auth/invites/{token}/accept` | `{"username", "password"}` | Single use; signs you in |

### Signed in

| Method | Route | Body | Notes |
|---|---|---|---|
| `GET` | `/auth/me` | — | Current user |
| `POST` | `/auth/logout` | — | Ends this session |
| `POST` | `/auth/me/password` | `{"current_password", "new_password"}` | Signs out your other sessions |
| `GET` | `/auth/me/tokens` | — | Your tokens (never the secret) |
| `POST` | `/auth/me/tokens` | `{"name"}` | Returns `{"token": "ptq_…"}` **once** |
| `PATCH` | `/auth/me/tokens/{id}` | `{"name"}` | Rename (the secret doesn't change) |
| `DELETE` | `/auth/me/tokens/{id}` | — | Revoke |

### Admin

| Method | Route | Body | Notes |
|---|---|---|---|
| `GET` | `/auth/users` | — | Everyone, with role, status and last sign-in |
| `PATCH` | `/auth/users/{username}` | `{"role"?, "disabled"?}` | Disabling signs the user out everywhere |
| `DELETE` | `/auth/users/{username}` | — | Also revokes their tokens |
| `GET` | `/auth/invites` | — | Pending invites |
| `POST` | `/auth/invites` | `{"role"}` | Returns `{"token", "path": "/?invite=…"}` |
| `DELETE` | `/auth/invites/{id}` | — | Revoke a pending invite |

The only active admin can't be demoted, disabled or deleted. Make someone else an admin first.

### WebSocket

`/ws/dashboard` accepts the session cookie (same-origin only) or `?token=ptq_…`. Unauthenticated connections are closed with code `4401`. Updates are pushed at most `LIVE_UPDATES_PER_SECOND` times a second (default 2).

### Calling the API from another website

The dashboard is served from the API's own address and needs no CORS. With the default `ALLOWED_ORIGINS=*`, browser code on other sites can call the API with an API token (`Authorization: Bearer ptq_…`), but **never** with a signed-in visitor's cookie. To let a specific site use cookies, list its exact origin: `ALLOWED_ORIGINS=https://admin.example.com`.

---

## Tracing (optional)

Set `OTEL_EXPORTER_OTLP_ENDPOINT` (and install `pytaskq[tracing]`, already in the Docker image) to send OpenTelemetry traces to Jaeger, Grafana Tempo, Honeycomb, Datadog and others. One trace follows a job from the request that queued it through every attempt: `POST /task/enqueue` → `enqueue send_receipt` → `run send_receipt` (one per attempt) → follow-up webhook jobs. A `traceparent` header sent to the API is continued, and `task.delay()` called inside your own app's span joins your trace.

Spans record task names, job ids, attempt numbers, worker ids, route patterns (`/task/{task_id}`) and status codes, never URLs, headers, bodies or job arguments. `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` are respected; `https://` endpoints use TLS.
