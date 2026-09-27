from typing import Literal
from typing import Any,List,Optional
from pydantic import BaseModel
# for incoming taks VALIDATION
class Taskloader(BaseModel):
    task_id:str
    task_name:str
    args:List[Any]
    retry_count:int = 0 
    task_type:str = "io"  # Legacy field — execution type is now looked up from the registry
    webhook_url: Optional[str] = None
    tenant_id: str = "unknown"  # Used for isolating stats, dlq, and rate limits
    client_ip: str = "unknown"  # Kept for logging/debugging only
    fence_token:int=0
    prev_delay: float = 1.0   # Decorrelated jitter: stores the last actual delay used for THIS task
    trace_carrier: dict = {}  # OpenTelemetry W3C traceparent — carries trace context across Redis queue
    
# for VALIDATION of result of task 
class Taskresult(BaseModel):
    task_id:str
    status:str
    result:str
    
class TaskRequest(BaseModel):
    task_name:str
    args: List[Any]=[]
    priority:Literal["high","default","low"]="default"

class WebhookRegistrationRequest(BaseModel):
    url: str