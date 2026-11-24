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

What a step receives:

- after a single step: that step's return value;
- after a group: a list of the group's results, **in the group's order** (not the order they finished);
- in the first stage: nothing extra.

Every step is an ordinary job: retries, timeouts, `max_concurrency`, `rate_limit`, progress, cancelling and the job list all apply. A step's job id is `<workflow id>.<stage>.<index>` (for example `wf-3f2a….1.0`), and its record has `workflow_id`.

Arguments are checked when the workflow starts, including the extra first argument of `.s()` steps after the first stage. A wrong call raises `TypeError` before anything runs.

## When a step fails

A step fails for good once its retries are used up (it becomes a dead letter). Then:

| `on_failure` | What happens |
|---|---|
| `"stop"` (default) | The workflow is **Failed**, with the reason. Later stages don't run (shown as *Skipped*). Steps of the same stage that are already running finish. |
| `"continue"` | The failed step counts as done with the result `None`, and the workflow carries on. It ends **PartlySucceeded**. |

After fixing the cause, **retry the failed steps**: the *Retry failed steps* button, `POST /workflows/{id}/retry`, or `await app.retry_workflow(id)`. They run again with the same ids and the same input, and when they succeed the workflow carries on from where it stopped. Replaying the step from the Dead letters tab does the same.

Cancelling one step (from the job list) cancels its workflow. Cancelling the workflow (`app.cancel_workflow(id)`, the button, `POST /workflows/{id}/cancel`) cancels its waiting steps and stops its running ones.

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

`"immutable": true` is `.si()`. Arguments are checked against the tasks the workers advertise (`422` if they can't work). See [API.md](API.md#workflows).

## What it guarantees

- **The next stage starts exactly once.** When a step finishes, one Lua script records its result and, only for the last step of its stage, moves the workflow on. Two steps finishing at the same moment can't both start the next stage, or neither.
- **No lost stages.** The next stage's steps have fixed ids, so queuing one twice is impossible. If a worker dies right after finishing a stage but before queuing the next one, the leader notices within a minute and queues the missing steps.
- **A step that runs twice** (a crash, a re-sent reply) is counted once.
- Results are passed through Redis as JSON, so they must fit `MAX_RESULT_BYTES` (1 MB) and be JSON-able (other values arrive as their text). For bigger data, return a location (a file path, an S3 key).
- A finished workflow is kept for `RESULT_TTL` (a day), like its jobs. The Workflows tab lists the newest 10,000.

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
