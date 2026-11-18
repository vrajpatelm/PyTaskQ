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
      "worker_count": 3
    }
    ```

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
  "delay_seconds": 0,          // optional; > 0 schedules the job
  "on_success_url": null       // optional webhook
}
```

Response: `{"task_id": "…", "status": "queued"}` (or `"scheduled"` with `execute_in_seconds`).

Errors:
* `404` — the task name isn't registered by any worker. The message lists the available tasks.
* `422` — invalid body (e.g. unknown priority).
* `429` — the queues are at `QUEUE_CAPACITY`.

`POST /task/schedule?delay_seconds=60` still works and takes the same body.

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

Non-string results are stored as JSON.

### Cancel — `POST /task/{task_id}/cancel`

Works for `Queued`, `Scheduled` and `RetryScheduled` jobs. Returns `409` for jobs that are running or finished, and `404` for unknown ones.

### History — `GET /tasks`

Query: `status`, `task_name`, `limit` (1–500). Newest first, drawn from the last `HISTORY_LIMIT` jobs.

### Registered tasks — `GET /tasks/registered`

What workers (or `pytaskq dashboard --app …`) advertised, including each parameter:

```json
{"tasks": [{"name": "send_receipt", "type": "io", "priority": "high", "max_retries": 3, "timeout": 300,
            "cron": null, "doc": "Email a receipt.",
            "params": [{"name": "order_id", "required": true, "annotation": "int"},
                       {"name": "email", "required": false, "default": "customer@example.com", "annotation": "str"}]}]}
```

### Workers — `GET /system/workers`

Each worker reports `hostname`, `pid`, `app`, `active`, `processed`, `failed`, `cpu_workers`, `io_workers`, `uptime_seconds`, `last_heartbeat_seconds_ago` and `status` (`healthy` / `degraded` / `dead`).

### Dead letters

Each `GET /dlq` entry now includes `error` (the last exception) and `failed_at`. `replay` and `retry_all` put jobs back on their **original priority queue** with a valid fence token.

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
| `POST` | `/auth/login` | `{"username", "password"}` | Throttled after 10 failures per user (or 30 per IP) for 15 minutes |
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

`/ws/dashboard` accepts the session cookie (same-origin only) or `?token=ptq_…`. Unauthenticated connections are closed with code `4401`.
