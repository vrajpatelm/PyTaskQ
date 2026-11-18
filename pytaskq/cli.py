"""
PyTaskQ command line
====================

    pytaskq worker    --app myapp.tasks:queue [--cpu-workers N] [--io-workers N] [--timeout S]
    pytaskq dashboard [--app myapp.tasks:queue] [--host 127.0.0.1] [--port 8000]
    pytaskq tasks     --app myapp.tasks:queue
    pytaskq ping

`--app` can also come from the PYTASKQ_APP environment variable.
"""

import argparse
import asyncio
import importlib
import logging
import os
import sys

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pytaskq")


class CLIError(Exception):
    pass


def load_app(spec: str):
    """Import `module:attribute` and return the PyTaskQ instance."""
    if not spec:
        raise CLIError("No app given. Use --app module:queue (or set PYTASKQ_APP), e.g. --app example_app:queue")
    sys.path.insert(0, os.getcwd())
    module_name, _, app_name = spec.partition(":")
    app_name = app_name or "queue"
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise CLIError(f"Could not import module '{module_name}': {e}") from e
    try:
        app = getattr(module, app_name)
    except AttributeError:
        raise CLIError(f"Could not find '{app_name}' in module '{module_name}'") from None
    if not hasattr(app, "TASKS"):
        raise CLIError(f"'{spec}' is not a PyTaskQ instance (no TASKS registry)")
    return app


def cmd_worker(args):
    app = load_app(args.app)
    from pytaskq import worker
    if args.cpu_workers:
        worker.CPU_WORKERS = args.cpu_workers
    if args.io_workers:
        worker.IO_WORKERS = args.io_workers
    if args.timeout is not None:
        worker.TASK_TIMEOUT = args.timeout
    worker.set_tasks(app.TASKS, app_name=args.app)
    log.info(f"Starting PyTaskQ worker for app: {args.app}")
    asyncio.run(worker.consumer_task())


def cmd_dashboard(args):
    import uvicorn
    if args.app:
        os.environ["PYTASKQ_APP"] = args.app
        load_app(args.app)  # fail fast with a clear message
    url = f"http://{'localhost' if args.host in ('0.0.0.0', '127.0.0.1') else args.host}:{args.port}"
    log.info(f"PyTaskQ dashboard → {url}   (API docs: {url}/docs)")
    uvicorn.run("pytaskq.main:app", host=args.host, port=args.port, reload=args.reload, log_level="info")


def cmd_tasks(args):
    app = load_app(args.app)
    specs = app.specs()
    rows = [(n, s) for n, s in sorted(specs.items()) if not s["internal"]]
    if not rows:
        print("No tasks registered.")
        return
    print(f"{len(rows)} task(s) in {args.app}:\n")
    for name, s in rows:
        params = ", ".join(
            p["name"] + ("" if p.get("required") else f"={p.get('default')!r}") for p in s["params"]
        )
        extras = [s["type"], f"priority={s['priority']}", f"retries={s['max_retries']}", f"timeout={s['timeout']:g}s"]
        if s["cron"]:
            extras.append(f"cron='{s['cron']}'")
        print(f"  {name}({params})")
        print(f"      {' · '.join(extras)}")
        if s["doc"]:
            print(f"      {s['doc']}")


def cmd_ping(args):
    from pytaskq.core.redis_client import make_client
    from pytaskq.core.config import settings

    async def go():
        client = make_client()
        try:
            await client.ping()
            workers = await client.zcard("active_workers")
            return workers
        finally:
            await client.aclose()

    target = settings.REDIS_URL or f"{settings.REDIS_HOST}:{settings.REDIS_PORT}"
    try:
        workers = asyncio.run(go())
    except Exception as e:
        raise CLIError(f"Cannot reach Redis at {target}: {e}") from e
    print(f"Redis OK at {target} — {workers} worker(s) registered")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pytaskq", description="PyTaskQ — background jobs for Python")
    sub = parser.add_subparsers(dest="command")
    default_app = os.environ.get("PYTASKQ_APP", "")

    w = sub.add_parser("worker", help="Start a worker that executes tasks")
    w.add_argument("--app", default=default_app, help="PyTaskQ instance, e.g. example_app:queue")
    w.add_argument("--cpu-workers", type=int, default=0, help="Processes for type='cpu' tasks (default: cores-1)")
    w.add_argument("--io-workers", type=int, default=0, help="Threads for type='io' tasks (default: cores*4)")
    w.add_argument("--timeout", type=float, default=None, help="Default task timeout in seconds (0 = none)")
    w.set_defaults(func=cmd_worker)

    d = sub.add_parser("dashboard", help="Start the web dashboard + REST API")
    d.add_argument("--app", default=default_app, help="Optional: show this app's tasks before any worker starts")
    d.add_argument("--host", default="127.0.0.1")
    d.add_argument("--port", type=int, default=8000)
    d.add_argument("--reload", action="store_true", help="Auto-reload on code changes (development)")
    d.set_defaults(func=cmd_dashboard)

    t = sub.add_parser("tasks", help="List the tasks registered in an app")
    t.add_argument("--app", default=default_app)
    t.set_defaults(func=cmd_tasks)

    p = sub.add_parser("ping", help="Check the Redis connection")
    p.set_defaults(func=cmd_ping)
    return parser


def main(argv=None):
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 1
    try:
        args.func(args)
    except CLIError as e:
        log.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    main()
