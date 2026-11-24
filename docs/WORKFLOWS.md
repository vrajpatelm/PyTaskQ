# Workflows

A workflow runs several jobs as one piece of work: some one after another, some at the same time, each passing its result to the next. You see it as one row in the dashboard's **Workflows** tab, with every step's state.

```python
from pytaskq import PyTaskQ, chain, group

app = PyTaskQ()

@app.task(type="io")
def download(url): ...                    # returns the file's location

@app.task(type="cpu")
def resize(path, size=64): ...            # gets download's result first

@app.task(type="io")
def publish(images, channel="web"): ...   # gets every resize result, as a list

wf = await app.workflow(
    chain(
        download.s("https://example.com/photo.jpg"),
        group(resize.s(size=64), resize.s(size=512), resize.s(size=2048)),
        publish.s(channel="cdn"),
    ),
    name="Photo thumbnails",
)
info = await app.wait_workflow(wf["workflow_id"], timeout=120)
info["status"]        # Succeeded, PartlySucceeded, Failed or Cancelled
```

## Building one

| | |
|---|---|
| `task.s(*args, **kwargs)` | A step. It receives the previous stage's result as its **first** argument, then `args`. |
| `task.si(*args, **kwargs)` | A step that doesn't receive the previous result. |
| `group(step, step, …)` | Steps that run at the same time. `group(resize.s(size=s) for s in sizes)` works too. |
| `chain(stage, stage, …)` | Stages one after another. A stage is a step or a group. |
| `.set(queue="gpu", priority="high")` | Run one step on another queue or priority than its task's default. |

Every step runs on **its task's own queue and priority** (`@app.task(queue="images", priority="high")`) unless `.set()` says otherwise, in every stage. Both are fixed when the workflow starts and checked then: a priority other than `high`, `default` or `low`, or an invalid queue name, raises `ValueError` and nothing is stored or queued.

What a step receives:

- after a single step: that step's return value;
- after a group: a list of the group's results, **in the group's order** (not the order they finished);
- in the first stage: nothing extra.

Every step is an ordinary job: retries, timeouts, `max_concurrency`, `rate_limit`, progress, cancelling, pausing its queue and the job list all apply. A step's job id is `<workflow id>.<stage>.<index>` (for example `wf-3f2a….1.0`), and its record has `workflow_id`.

The previous stage's result isn't copied into the next stage's jobs: a step reads it from the workflow right before it runs. So a group of 20 big results followed by a group of 10 steps stores those results once, not ten times, and the job list shows each step's own arguments.

Arguments are checked when the workflow starts, including the extra first argument of `.s()` steps after the first stage. A wrong call raises `TypeError` before anything runs.

## When a step fails

A step fails for good once its retries are used up (it becomes a dead letter). Then:

| `on_failure` | What happens |
|---|---|
| `"stop"` (default) | The workflow is **Failed**, with the reason. Later stages don't run (shown as *Skipped*). The other steps of the same stage still run and their results are kept. |
| `"continue"` | The failed step counts as done with the result `None`, and the workflow carries on. It ends **PartlySucceeded**. |

After fixing the cause of a **Failed** workflow, **retry the failed steps**: the *Retry failed steps* button, `POST /workflows/{id}/retry`, or `await app.retry_workflow(id)`. They run again with the same ids and the same input, and once they succeed the workflow carries on from where it stopped, also when the other steps of that stage finished after the failure. Replaying the step from the Dead letters tab does the same.

With `"continue"` there's nothing to retry: the workflow already went on with `None`. `retry_workflow()` returns 0 (the API answers `409`). Replaying such a step from the Dead letters tab runs the job, but doesn't change the workflow.

**Cancelling** the workflow (`app.cancel_workflow(id)`, the button, `POST /workflows/{id}/cancel`) works while it's Running or Failed: its waiting steps are cancelled, running ones are stopped, and no later step ever starts, even one whose stage was finishing at that very moment. Cancelling one step (from the job list) cancels its workflow the same way, siblings included.

## From other languages

```bash
curl -X POST https://jobs.example.com/workflows -H "Authorization: Bearer ptq_..." \
  -H "Content-Type: application/json" -d '{
    "name": "Photo thumbnails",
    "on_failure": "stop",
    "steps": [
      {"task": "download", "args": ["https://example.com/photo.jpg"]},
      {"group": [{"task": "resize", "kwargs": {"size": 64}}, {"task": "resize", "kwargs": {"size": 512}}]},
      {"task": "publish", "kwargs": {"channel": "cdn"}},
      {"task": "notify", "args": ["done"], "immutable": true}
    ]}'
```

`"immutable": true` is `.si()`. A step may also have `"priority"` and `"queue"`. Everything is checked against the tasks the workers advertise before anything starts: wrong arguments, a bad priority or a queue nobody serves get `422`, an unknown task `404`.

Send an `Idempotency-Key` header to make a retried request safe: the same key (within `IDEMPOTENCY_TTL`, a day) returns the first workflow with `"duplicate": true` instead of starting a second one. In Python: `app.workflow(..., idempotency_key="order-1001")`. See [API.md](API.md#workflows).

## What it guarantees

- **The next stage starts exactly once.** When a step finishes, one Lua script records its result and, only for the last step of its stage, moves the workflow on. Two steps finishing at the same moment can't both start the next stage, or neither.
- **No lost stages.** The next stage's steps have fixed ids, so queuing one twice is impossible. If a worker dies right after finishing a stage but before queuing the next one, the leader notices within a minute and queues the missing steps. If it dies after saving a step's result but before telling the workflow, the leader counts that step from its job record (with its result) instead of running it again.
- **A step that runs twice** (a crash, a re-sent reply) is counted once, and a step that's already counted is never run again by the leader, even after its job record expired or was trimmed.
- **Cancelled means cancelled.** Once a workflow is cancelled, none of its remaining steps starts.
- Results are passed through Redis as JSON, so they must fit `MAX_RESULT_BYTES` (1 MB) and be JSON-able (other values arrive as their text). For bigger data, return a location (a file path, an S3 key).
- A finished, failed or cancelled workflow is kept for `RESULT_TTL` (a day), like its jobs; retrying a failed one keeps it again until it finishes. The Workflows tab lists the newest 10,000.

## How it compares

If you know Celery or BullMQ:

| | Celery canvas | BullMQ flows | PyTaskQ workflows |
|---|---|---|---|
| Building blocks | `chain`, `group`, `chord` | parent and children | `chain` and `group` (a group followed by a step is a chord) |
| Needs a result backend | Yes, chords poll it | No | No: results live next to the jobs |
| See a running workflow | Separate tasks in Flower | A separate dashboard | Workflows tab: stages, steps, reason, retry and cancel buttons |
| After a failure | Usually start the chain again | Retry the failed child job | Retry the failed steps; the workflow resumes |
| Choose stop or continue | Error callbacks (`link_error`) | `failParentOnFailure` / `ignoreDependencyOnFailure` per child | `on_failure="stop"` / `"continue"` per workflow |
| From other languages | Via the broker's protocol | Node.js | One HTTP request |

Not there yet: groups of chains inside a group (each item of a group is one step), and starting a workflow from inside a step.
