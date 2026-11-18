from typing import Literal, Any, Dict, List, Optional
from pydantic import BaseModel, Field


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
    max_retries: Optional[int] = None  # per-task override (else registry / settings)
    timeout: Optional[float] = None    # per-task override (else registry / settings)
    trace_carrier: dict = Field(default_factory=dict)
    on_success_url: Optional[str] = None  # If set, worker enqueues a webhook delivery task on success


class Taskresult(BaseModel):
    task_id: str
    status: str
    result: str


class TaskRequest(BaseModel):
    task_name: str
    args: List[Any] = Field(default_factory=list)
    priority: Literal["high", "default", "low"] = "default"
    idempotency_key: Optional[str] = None
