from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class Taskloader(BaseModel):
    """A task payload as it travels through Redis."""
    task_id: str
    task_name: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    retry_count: int = 0
    task_type: str = "io"
    fence_token: int = 0
    prev_delay: float = 1.0
    priority: str = "default"          # preserved across retries
    queue: str = "default"             # named queue; preserved across retries (older jobs have none)
    bounces: int = 0                   # times handed back because the worker that took it lacked the task
    handed_back_since: Optional[float] = None   # when the first hand-back happened (gives up after a while)
    max_retries: Optional[int] = None  # per-task override (else registry / settings)
    timeout: Optional[float] = None    # per-task override (else registry / settings)
    trace_carrier: dict = Field(default_factory=dict)
    on_success_url: Optional[str] = None  # If set, worker enqueues a webhook delivery task on success

    @field_validator("queue", mode="before")
    @classmethod
    def _known_queue_name(cls, v):
        # A hand-edited or corrupt queue name falls back to "default" instead of a stray Redis key.
        from pytaskq.core.queueing import payload_queue
        return payload_queue({"queue": v})


class Taskresult(BaseModel):
    task_id: str
    status: str
    result: str
