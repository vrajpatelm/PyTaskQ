# API Reference

This document details the REST API endpoints exposed by the Web API Server ([src/app.py](file:///c:/Users/VRAJ/redis/src/app.py)). By default, the server runs on `http://localhost:8000`.

---

##  Task Endpoints

### 1. Matrix Multiplication (CPU Task)
Queue a heavy matrix multiplication calculation.

*   **URL**: `/task/mul`
*   **Method**: `GET`
*   **Query Parameters**:
    *   `size` (integer, optional): The dimension of the matrix to multiply (e.g. `size=50` creates `50x50` matrices). Defaults to `10`.
*   **Response (200 OK)**:
    ```json
    {
      "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
      "status": "queued"
    }
    ```

---

### 2. Send Email (I/O Task)
Queue an email delivery task.

*   **URL**: `/task/send_email`
*   **Method**: `POST`
*   **Content-Type**: `application/x-www-form-urlencoded`
*   **Request Body**:
    *   `email` (string, required): Recipient's email address.
    *   `title` (string, required): Email subject line.
    *   `body` (string, required): Email content body.
*   **Response (200 OK)**:
    ```json
    {
      "task_id": "42ba7f52-8703-455f-8fe0-2b28c8de1e2b",
      "status": "queued"
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
            "result": "{'result': 'Email sended to vraj@example.com'}"
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
            "error": "Failed after 3 retries. Last error: SMTPAuthenticationError..."
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
    [
      {
        "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
        "task_name": "send_email",
        "args": ["invalid-email", "Hello", "Body"],
        "retry_count": 3,
        "task_type": "io"
      }
    ]
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
        "task_name": "send_email",
        "args": ["invalid-email", "Hello", "Body"],
        "retry_count": 0,
        "task_type": "io"
      }
    }
    ```

---

### 3. Purge Task
Remove a single task from the DLQ permanently.

*   **URL**: `/dlq/purge/{task_id}`
*   **Method**: `POST`
*   **Response (200 OK)**:
    ```json
    {
      "message": "tasked is Deleted",
      "task": {
        "task_id": "8b51d8b7-6ff9-49ee-ae08-e7e0e7a8dfcf",
        "task_name": "send_email",
        "args": ["invalid-email", "Hello", "Body"],
        "retry_count": 3,
        "task_type": "io"
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
