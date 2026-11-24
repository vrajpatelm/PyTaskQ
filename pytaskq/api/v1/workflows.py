from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from pytaskq import workflows
from pytaskq.app import argument_error
from pytaskq.core import auth, queueing, redis_client
from pytaskq.core.dependencies import require

router = APIRouter(prefix="/workflows", tags=["workflows"], dependencies=[Depends(require("viewer"))])


@router.get("")
async def list_workflows(status: Optional[str] = None, limit: int = Query(50, ge=1, le=500)):
    """The newest workflows: name, status, how many steps are done or failed."""
    return {"workflows": await workflows.recent(redis_client.r, limit=limit, status_filter=status)}


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str):
    """A workflow with every stage and step (status, times, error)."""
    info = await workflows.status(redis_client.r, workflow_id)
    if info is None:
        raise HTTPException(status_code=404, detail="No workflow with that id (or it finished long ago).")
    return info


@router.post("")
async def create_workflow(
    steps: list[Any] = Body(..., description='[{"task": "fetch", "args": [...]}, {"group": [{...}, {...}]}, ...]'),
    name: Optional[str] = Body(None),
    on_failure: str = Body("stop"),
    user: auth.User = Depends(require("operator")),
):
    """
    Start a workflow from JSON, for code that isn't Python. Each item of `steps` is a
    stage: one step, or `{"group": [step, step, …]}` for steps that run at the same time.
    A step receives the previous stage's result as its first argument unless it has
    `"immutable": true`. Arguments are checked against the tasks the workers advertise.
    """
    try:
        plan = workflows.plan_from_json(steps)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    specs = {t["name"]: t for t in await queueing.registered_tasks(redis_client.r)}
    for i, stage in enumerate(plan):
        for step in stage:
            if step["task"].startswith("_"):
                raise HTTPException(status_code=403, detail=f"'{step['task']}' is internal to PyTaskQ.")
            spec = specs.get(step["task"])
            if specs and spec is None:
                raise HTTPException(status_code=404, detail=f"Unknown task '{step['task']}'.")
            if spec:
                if not step.get("queue") and spec.get("queue"):
                    step["queue"] = spec["queue"]
                previous = [None] if i > 0 and not step.get("immutable") else []
                problem = argument_error(spec.get("params"), previous + step["args"], step["kwargs"])
                if problem:
                    raise HTTPException(status_code=422, detail=f"Wrong arguments for step '{step['task']}': {problem}")
    try:
        return await workflows.start(redis_client.r, plan, name=name, on_failure=on_failure,
                                     created_by=user.username)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None


@router.post("/{workflow_id}/cancel")
async def cancel_workflow(workflow_id: str, user: auth.User = Depends(require("operator"))):
    """Cancel a running workflow: its waiting steps won't run, running ones are stopped."""
    before = await workflows.cancel(redis_client.r, workflow_id, by=user.username)
    if before is None:
        raise HTTPException(status_code=404, detail="No workflow with that id.")
    if before != workflows.RUNNING:
        raise HTTPException(status_code=409, detail=f"The workflow already finished ({before}).")
    return {"workflow_id": workflow_id, "status": workflows.CANCELLED}


@router.post("/{workflow_id}/retry")
async def retry_workflow(workflow_id: str, user: auth.User = Depends(require("operator"))):
    """Run the failed steps again (same ids). When they succeed, the workflow carries on."""
    retried = await workflows.retry_failed(redis_client.r, workflow_id)
    if not retried:
        raise HTTPException(status_code=409, detail="Nothing to retry: no failed steps.")
    return {"workflow_id": workflow_id, "retried": retried}
