from typing import Literal, Any, List, Optional
from pydantic import BaseModel

class Taskloader(BaseModel):
    task_id: str
    task_name: str
    args: List[Any]
    retry_count: int = 0 
    task_type: str = "io"
    fence_token: int = 0
    prev_delay: float = 1.0
    trace_carrier: dict = {}
    
class Taskresult(BaseModel):
    task_id: str
    status: str
    result: str
    
class TaskRequest(BaseModel):
    task_name: str
    args: List[Any] = []
    priority: Literal["high","default","low"] = "default"
    idempotency_key: Optional[str] = None
