"""
Load test for a running PyTaskQ server.

Simulates people and services using PyTaskQ at the same time:
  * queueing jobs                       POST /task/enqueue   (most traffic)
  * checking a job they queued          GET  /task/{id}
  * looking at the dashboard data       GET  /tasks, /metrics

Run it from YOUR computer, not from the server itself:

    pip install locust
    locust -f loadtest/locustfile.py --host https://your-domain --token ptq_...

or put these two lines in the .env file of the folder you run it from:
    LOADTEST_HOST=https://your-domain
    PYTASKQ_TOKEN=ptq_...
and just run:  locust -f loadtest/locustfile.py

then open http://localhost:8089, choose the number of users and press Start.

Settings (command-line option, environment variable, or a line in .env):
    --host  / LOADTEST_HOST   address of your PyTaskQ server. Required.
    --token / PYTASKQ_TOKEN   API token from Settings → API tokens (operator or admin). Required.
    PYTASKQ_TASK    task to queue (default: notify_user, from the example app)
    PYTASKQ_KWARGS  JSON kwargs for that task (default: {"user_id": 1})
"""

import json
import os
import random
import time

from locust import FastHttpUser, between, events, task


def _read_dotenv(path=".env"):
    """Load KEY=VALUE lines from .env (without overriding real environment variables)."""
    try:
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


_read_dotenv()

TASK_NAME = os.environ.get("PYTASKQ_TASK", "notify_user")
TASK_KWARGS = json.loads(os.environ.get("PYTASKQ_KWARGS", '{"user_id": 1}'))

_start = {}
TOKEN = ""


@events.init_command_line_parser.add_listener
def _add_options(parser):
    parser.add_argument("--token", type=str, env_var="PYTASKQ_TOKEN", default="",
                        help="PyTaskQ API token (ptq_…) from Settings → API tokens")


def _metrics(environment):
    """Read the server's job counters with a plain request (not counted in results)."""
    import urllib.request
    req = urllib.request.Request(
        environment.host.rstrip("/") + "/metrics",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


@events.test_start.add_listener
def _on_start(environment, **_):
    global TOKEN
    host = (environment.host or os.environ.get("LOADTEST_HOST", "")).strip().rstrip("/")
    if not host:
        raise SystemExit("No server address. Add  --host https://your-server  (or LOADTEST_HOST in .env).")
    environment.host = host
    PyTaskQUser.host = host
    TOKEN = (getattr(environment.parsed_options, "token", "") or "").strip().strip('"').strip("'")
    if not TOKEN:
        raise SystemExit("No API token. Add  --token ptq_…  to the locust command "
                         "(create one in the dashboard: Settings → API tokens).")
    if not TOKEN.startswith("ptq_"):
        raise SystemExit("That doesn't look like a PyTaskQ API token (they start with ptq_).")
    # Every simulated user sends the token (users are created after this runs)
    PyTaskQUser.default_headers = {"Authorization": f"Bearer {TOKEN}"}
    try:
        m = _metrics(environment)
    except Exception as e:
        raise SystemExit(f"Can't reach {environment.host} with that token: {e}")
    _start.update(time=time.time(), completed=m["completed_total"], failed=m["failed_total"])
    print(f"\nStarting: {m['worker_count']} worker(s), {m['queue_total']} job(s) already queued, "
          f"queue capacity {m['queue_capacity']}\n")


@events.test_stop.add_listener
def _on_stop(environment, **_):
    if not _start:
        return
    m = _metrics(environment)
    secs = max(1e-9, time.time() - _start["time"])
    done = m["completed_total"] - _start["completed"]
    print(
        "\n── Job processing during the test ──────────────────────\n"
        f"  jobs finished by workers : {done}  ({done / secs:.1f} per second)\n"
        f"  job failures             : {m['failed_total'] - _start['failed']}\n"
        f"  still waiting in queue   : {m['queue_total']}  (workers couldn't keep up if this keeps growing)\n"
        f"  workers                  : {m['worker_count']}\n"
    )


class PyTaskQUser(FastHttpUser):
    wait_time = between(0.5, 2)
    default_headers = {}

    def on_start(self):
        self.my_jobs = []

    @task(6)
    def queue_a_job(self):
        kwargs = dict(TASK_KWARGS)
        if "user_id" in kwargs:
            kwargs["user_id"] = random.randint(1, 100_000)
        with self.client.post("/task/enqueue", json={"task_name": TASK_NAME, "kwargs": kwargs},
                              name="POST /task/enqueue", catch_response=True) as resp:
            if resp.status_code == 200:
                self.my_jobs = (self.my_jobs + [resp.json()["task_id"]])[-20:]
                resp.success()
            elif resp.status_code == 429:
                resp.failure("429 queue full: workers are not keeping up (QUEUE_CAPACITY reached)")
            elif resp.status_code in (401, 403):
                resp.failure(f"{resp.status_code}: token invalid or role too low (needs operator)")
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(3)
    def check_my_job(self):
        if self.my_jobs:
            self.client.get(f"/task/{random.choice(self.my_jobs)}", name="GET /task/{id}")

    @task(2)
    def view_recent_jobs(self):
        self.client.get("/tasks?limit=50", name="GET /tasks")

    @task(1)
    def view_metrics(self):
        self.client.get("/metrics", name="GET /metrics")
