# API Reference

This document details the REST API endpoints exposed by the Web API Server ([src/app.py](../src/app.py)). By default, the server runs on `http://localhost:8000`.

---

## Authentication

Most endpoints require an API Key. Pass it in the `Authorization` header as a Bearer token:

```http
Authorization: Bearer sk_your_api_key_here
```

To generate an API key, use the Admin Routes below (requires `MASTER_KEY` from your `.env`).

> **Note:** `webhook_url` is **not accepted** in task submissions — URLs must be registered once via `/webhooks/register`. Submitting `webhook_url` in a task body returns `400 Bad Request`.

---

## Idempotency

PyTaskQ supports two layers of duplicate request protection:

### Layer 1: `Idempotency-Key` (Recommended)

Send a unique key with your request — either as an HTTP header **or** as the body field `idempotency_key`. If the server sees the same key again within 24 hours, it returns the **cached response** (same `task_id`) without enqueuing a duplicate task.

```http
POST /task/enqueue
Authorization: Bearer sk_your_api_key_here
Idempotency-Key: my-unique-key-123
Content-Type: application/json

{"task_name": "matrix_multiply", "args": [100]}
```

**Retry behavior:**
| Call | What happens |
|------|-------------|
| 1st call | Task enqueued, response cached under key |
| 2nd call (same key) | Returns cached response — no duplicate task created |
| 2nd call (different key) | New task enqueued normally |

**Scoping:** Keys are scoped per **tenant**. Your key `"abc"` won't collide with another tenant's key `"abc"`.

### Layer 2: Automatic Safety Net (No Key Required)

If no idempotency key is sent, the server hashes the request content (`task_name + args + tenant_id`). If an identical request arrives within **5 seconds** (env: `DEDUP_TTL`), it is rejected with `409 Conflict`. This catches double-clicks and network retries automatically.

### Rate-limit interaction

Requests caught by either idempotency layer do **not** consume rate-limit slots — only confirmed-fresh requests count toward `RATE_LIMIT_PER_MINUTE`.

---

## Task Endpoints

### 1. Enqueue Task
Submit a task for immediate execution with optional priority routing.

*   **URL**: `/task/enqueue`
*   **Method**: `POST`
*   **Auth**: API key required
*   **Content-Type**: `application/json`
*   **Request Body**:
    *   `task_name` (string, required): One of `matrix_multiply`, `url_health_check`, `generate_csv_report`, `resize_image`.
    *   `args` (array, optional): Arguments passed to the task handler.
    *   `priority` (string, optional): `"high"`, `"default"`, or `"low"`. Defaults to `"default"`.
    *   `idempotency_key` (string, optional): Alternative to the `Idempotency-Key` header.
*   **Example Request**:
    ```json
    {
      "task_name": "matrix_multiply",
      "args": [100],
      "priority": "high"
    }
    ```
*   **Response (200 OK)**:
    ```json
    {
      "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
      "status": "queued"
    }
    ```
*   **Errors**:
    *   `400` — unknown task name, or `webhook_url` present in body
    *   `409` — duplicate request (content-hash window)
    *   `422` — `matrix_multiply` size exceeds 1000
    *   `429` — rate limit exceeded, or backpressure (queue capacity reached)

---

### 2. Schedule Task
Submit a task to be executed after a delay.

*   **URL**: `/task/schedule`
*   **Method**: `POST`
*   **Auth**: API key required
*   **Query Parameters**:
    *   `delay_seconds` (integer, optional): How many seconds to wait before executing. Defaults to `60`.
*   **Request Body**: Same as `/task/enqueue`.
*   **Response (200 OK)**:
    ```json
    {
      "task_id": "42ba7f52-8703-455f-8fe0-2b28c8de1e2b",
      "status": "scheduled",
      "execute_in_seconds": 60
    }
    ```

---

### 3. Get Task Status/Result
Fetch the current state, execution output, or error info for a specific task.

*   **URL**: `/task/{task_id}`
*   **Method**: `GET`
*   **Auth**: API key required
*   **URL Parameters**:
    *   `task_id` (string, required): The UUID of the task.
*   **Tenant isolation**: The stored task must carry **your** `tenant_id`. A mismatch — or a missing tenant field on the stored hash — returns `403 Access denied`. The endpoint never falls back to anonymous-readable behavior.
*   **Response (200 OK)**:
    *   *If task has completed successfully*:
        ```json
        {
          "result": {
            "task_id": "42ba7f52-8703-455f-8fe0-2b28c8de1e2b",
            "status": "Success",
            "tenant_id": "t_98765432",
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
            "tenant_id": "t_98765432",
            "retry_count": "1",
            "error": "Error: Connection refused..."
          }
        }
        ```
    *   *If task is in Dead Letter Queue (failed 3 times)*:
        ```json
        {
          "result": {
            "task_id": "52ba8f52-8703-455f-8fe0-2b28c8de1e2f",
            "status": "DeadLetter",
            "tenant_id": "t_98765432",
            "error": "Failed after 3 retries. Last error: ..."
          }
        }
        ```
*   **Errors**: `404` — task not found · `403` — belongs to another tenant

---

## Metrics Endpoint

### Get Queue Metrics
Returns live per-tenant queue and worker statistics.

*   **URL**: `/metrics`
*   **Method**: `GET`
*   **Auth**: API key required
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
      "rate_limit_used": 3,
      "rate_limit_max": 100,
      "worker_count": 1
    }
    ```
    `rate_limit_max` is the live value of `RATE_LIMIT_PER_MINUTE` — the same constant the enforcer uses.

---

## Dead Letter Queue (DLQ) Management

All DLQ data is scoped to the authenticated tenant (`dlq:{tenant_id}`).

### 1. View DLQ Tasks
Retrieve all tasks currently stored in the Dead Letter Queue.

*   **URL**: `/dlq`
*   **Method**: `GET`
*   **Response (200 OK)**:
    ```json
    {
      "tasks": [
        {
          "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
          "task_name": "matrix_multiply",
          "args": [500],
          "retry_count": 3,
          "priority": "default",
          "tenant_id": "t_98765432"
        }
      ]
    }
    ```

---

### 2. Replay Task
Re-enqueue a failed DLQ task back to its original priority queue (resets retry count to `0`).

*   **URL**: `/dlq/replay/{task_id}`
*   **Method**: `POST`
*   **Response (200 OK)**:
    ```json
    {
      "message": "Task replayed successfully",
      "task": { "...": "full task object" }
    }
    ```
*   **Error**: `404` — task_id not found in your DLQ

---

### 3. Purge Task
Remove a single task from the DLQ permanently.

*   **URL**: `/dlq/purge/{task_id}`
*   **Method**: `POST`
*   **Response (200 OK)**: `{ "message": "tasked is Deleted", "task": { ... } }`
*   **Error**: `404` — task_id not found

### 4. Clear Entire DLQ
Remove all tasks from the Dead Letter Queue (idempotent — returns 200 even when already empty).

*   **URL**: `/dlq/purge_all`
*   **Method**: `POST`
*   **Response (200 OK)**: `{ "message": "Enitre dlq is cleared" }`

---

## Webhook Routes

### Register Webhook
**POST** `/webhooks/register`

Registers the tenant's callback URL and generates a signing secret (returned once — store it).

**Request Body:**
```json
{ "url": "https://yourapp.example.com/hooks/pytaskq" }
```

**Response:**
```json
{
  "status": "registered",
  "url": "https://yourapp.example.com/hooks/pytaskq",
  "secret": "3f9c...hex"
}
```

### Webhook Info
**GET** `/webhooks/info` — returns `{ "registered": false }` or the registered URL and secret.

**Delivery behavior:** when a task succeeds and a webhook is registered, the worker enqueues a dedicated `_deliver_webhook` task (itself retryable, DLQ after 3 failures). Deliveries include an `X-PyTaskQ-Signature: sha256=HMAC_SHA256(secret, payload)` header — verify it before trusting the body.

---

## Admin Routes

These routes require the `MASTER_KEY` defined in your `.env` file, passed as a Bearer token. If `MASTER_KEY` is unset on the server, all admin routes return `500` (admin surface disabled). Comparison uses constant-time `secrets.compare_digest`.

### Create API Key
**POST** `/admin/keys/create`

**Request Body:**
```json
{
  "label": "Frontend Dashboard"
}
```

**Response:**
```json
{
  "api_key": "sk_abc123...",
  "tenant_id": "t_987654...",
  "label": "Frontend Dashboard"
}
```

### List API Keys
**GET** `/admin/keys`

Returns all active API keys and their associated `tenant_id`s.

### Revoke API Key
**DELETE** `/admin/keys/revoke/{api_key}`

Immediately invalidates the key (its tenant's queued tasks and stats remain in Redis).

---

## Ops Routes

### Health Check
**GET** `/health` — no auth. Returns `{ "status": "ok", "redis": "connected" }` or `503` with `status: "degraded"`. Used by Docker health checks and load balancers.
