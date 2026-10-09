# PyTaskQ — API & Dashboard Reference

While PyTaskQ primarily operates over Redis, it optionally ships with a lightweight FastAPI server that hosts a beautiful Real-Time React Dashboard and exposes REST endpoints for queue observability and management.

By default, the server runs on `http://localhost:8000`.

---

## 1. Starting the API & Dashboard

You can start the dashboard and API using standard `uvicorn`:

```bash
uvicorn pytaskq.main:app --reload
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
