# API Reference

This document details the REST API endpoints exposed by the Web API Server ([src/app.py](file:///c:/Users/VRAJ/Distributed-Queue/src/app.py)). By default, the server runs on `http://localhost:8000`.

---

## Authentication

Most endpoints require an API Key. Pass it in the `Authorization` header as a Bearer token:

```http
Authorization: Bearer sk_your_api_key_here
```

To generate an API key, use the Admin Routes below (requires `MASTER_KEY` from your `.env`).

---

## Idempotency

PyTaskQ supports two layers of duplicate request protection:

### Layer 1: `Idempotency-Key` Header (Recommended)

Send a unique key with your request. If the server sees the same key again within 24 hours, it returns the **cached response** (same `task_id`) without enqueuing a duplicate task.

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

**Scoping:** Keys are scoped per client IP. Your key `"abc"` won't collide with another user's key `"abc"`.

### Layer 2: Automatic Safety Net (No Header Required)

If no `Idempotency-Key` is sent, the server hashes the request content (`task_name + args + client IP`). If an identical request arrives within **5 seconds**, it is rejected with `409 Conflict`. This catches double-clicks and network retries automatically.

---

##  Task Endpoints

### 1. Enqueue Task
Submit a task for immediate execution with optional priority routing.

*   **URL**: `/task/enqueue`
*   **Method**: `POST`
*   **Content-Type**: `application/json`
*   **Request Body**:
    *   `task_name` (string, required): One of `matrix_multiply`, `url_health_check`, `generate_csv_report`, `resize_image`.
    *   `args` (array, optional): Arguments passed to the task handler.
    *   `priority` (string, optional): `"high"`, `"default"`, or `"low"`. Defaults to `"default"`.
    *   `webhook_url` (string, optional): URL to receive a POST callback with the result when the task completes.
*   **Example Request**:
    ```json
    {
      "task_name": "matrix_multiply",
      "args": [100],
      "priority": "high",
      "webhook_url": "https://example.com/callback"
    }
    ```
*   **Response (200 OK)**:
    ```json
    {
      "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
      "status": "queued"
    }
    ```
*   **Error (400)**:
    ```json
    {
      "detail": "Unknown Task send_email, Available Task: ['matrix_multiply', 'url_health_check', 'generate_csv_report', 'resize_image']"
    }
    ```

---

### 2. Schedule Task
Submit a task to be executed after a delay.

*   **URL**: `/task/schedule`
*   **Method**: `POST`
*   **Content-Type**: `application/json`
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
    *   *If task is in Dead Letter Queue (failed 3 times)*:
        ```json
        {
          "result": {
            "task_id": "52ba8f52-8703-455f-8fe0-2b28c8de1e2f",
            "status": "DeadLetter",
            "error": "Failed after 3 retries. Last error: ..."
          }
        }
        ```

---

##  Metrics Endpoints

### Get Queue Metrics
Returns the count of active items inside different queue queues.

*   **URL**: `/metrics`
*   **Method**: `GET`
*   **Response (200 OK)**:
    ```json
    {
      "pending": 0,
      "processing": 0,
      "delayed": 0,
      "dlq": 0
    }
    ```

---

##  Dead Letter Queue (DLQ) Management

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
          "client_ip": "127.0.0.1"
        }
      ]
    }
    ```

---

### 2. Replay Task
Re-enqueue a failed DLQ task back to the main queue (resets retry count to `0`).

*   **URL**: `/dlq/replay/{task_id}`
*   **Method**: `POST`
*   **Response (200 OK)**:
    ```json
    {
      "message": "Task replayed successfully",
      "task": {
        "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
        "task_name": "matrix_multiply",
        "args": [500],
        "retry_count": 0,
        "priority": "default",
        "client_ip": "127.0.0.1"
      }
    }
    ```

---

### 3. Purge Task
Remove a single task from the DLQ permanently.
---

## Admin Routes

These routes require the `MASTER_KEY` defined in your `.env` file, passed as a Bearer token.

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
*   **URL**: `/dlq/purge/{task_id}`
*   **Method**: `POST`
*   **Response (200 OK)**:
    ```json
    {
      "message": "tasked is Deleted",
      "task": {
        "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
        "task_name": "matrix_multiply",
        "args": [500],
        "retry_count": 3,
        "priority": "default",
        "client_ip": "127.0.0.1"
      }
    }
    ```

---

### 4. Clear Entire DLQ
Remove all tasks from the Dead Letter Queue.

*   **URL**: `/dlq/purge_all`
*   **Method**: `POST`
*   **Response (200 OK)**:
    ```json
    {
      "message": "Enitre dlq is cleared"
    }
    ```
