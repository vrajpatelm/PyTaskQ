# Next Tasks

- [ ] Implement ProcessPoolExecutor for slightly faster execution of tasks than threads

- [ ] **Multi-Worker Horizontal Scaling**
  - Currently the system is designed for **single-worker deployment**
  - Problem: On startup, crash recovery moves ALL items from `processing_queue` back to `task_queue`. If a second worker starts while the first is still alive, it yanks the first worker's in-progress tasks and re-queues them — causing **duplicate execution**
  - Solution: Give each worker its own processing queue (e.g. `processing:{worker_id}`) and add a heartbeat mechanism so workers only reclaim tasks from **dead** workers, not live ones
  - Reference: `worker.py` lines 71-82 (crash recovery loop)

- [ ] **Enable Redis AOF Persistence**
  - Set `appendonly yes` and `appendfsync everysec` in Redis config
  - Without this, a Redis restart loses all pending tasks, delayed retries, and DLQ entries — breaking the at-least-once delivery guarantee
  - AOF rewrite keeps the file compact since task queues are transient (pushed, consumed, removed)