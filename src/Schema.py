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
    
# for VALIDATION of result of task 
class Taskresult(BaseModel):
    task_id:str
    status:str
    result:str
    
class TaskRequest(BaseModel):
    task_name:str
    args: List[Any]=[]
    webhook_url: Optional[str] = None