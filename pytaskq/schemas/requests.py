from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator


class Taskloader(BaseModel):
    """A task payload as it travels through Redis.

    Everything is checked when a worker reads a job: a hand-written or corrupt
    payload (a negative retry count would give extra retries, an unknown
    priority would land on a stray list) becomes a dead letter instead of running.
    """
    task_id: str = Field(min_length=1, max_length=200)
    task_name: str = Field(min_length=1, max_length=200)
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    retry_count: int = Field(0, ge=0)
    task_type: str = "io"
    fence_token: int = Field(0, ge=0)
    prev_delay: float = Field(1.0, ge=0, allow_inf_nan=False)
    priority: Literal["high", "default", "low"] = "default"   # preserved across retries
    queue: str = "default"             # named queue; preserved across retries (older jobs have none)
    bounces: int = Field(0, ge=0)      # times handed back because the worker that took it lacked the task
    handed_back_since: Optional[float] = None   # when the first hand-back happened (gives up after a while)
    waits: int = Field(0, ge=0)        # times it waited for its task's max_concurrency (backs off a little more each time)
    rate_slot: Optional[float] = Field(None, allow_inf_nan=False)   # start time reserved under its task's rate_limit
    max_retries: Optional[int] = Field(None, ge=0)                     # per-task override (else registry / settings)
    timeout: Optional[float] = Field(None, ge=0, allow_inf_nan=False)  # per-task override (else registry / settings)
    trace_carrier: dict = Field(default_factory=dict)
    workflow: Optional[Dict[str, Any]] = None   # {"id", "stage", "index"} for a step of a workflow
    after_timeout: bool = False   # a retry of an IO attempt whose thread may still run: waits until it returns
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
