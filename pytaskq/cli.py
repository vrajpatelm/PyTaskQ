"""
PyTaskQ command line
====================

    pytaskq worker    --app myapp.tasks:queue [--cpu-workers N] [--io-workers N] [--timeout S]
    pytaskq dashboard [--app myapp.tasks:queue] [--host 127.0.0.1] [--port 8000]
    pytaskq tasks     --app myapp.tasks:queue
    pytaskq ping
    pytaskq users list | create NAME | reset-password NAME | set-role NAME ROLE | delete NAME | setup-code

`--app` can also come from the PYTASKQ_APP environment variable.
`users` talks to Redis directly, so it works even when you're locked out of the dashboard.
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


def _with_redis(fn):
    """Run an async function with a fresh Redis client (CLI helpers)."""
    from pytaskq.core.redis_client import make_client

    async def go():
        client = make_client()
        try:
            return await fn(client)
        finally:
            await client.aclose()

    return asyncio.run(go())


def _ask_password(given: str = "") -> str:
    if given:
        return given
    import getpass
    first = getpass.getpass("New password: ")
    if first != getpass.getpass("Repeat password: "):
        raise CLIError("Passwords don't match.")
    return first


def cmd_users(args):
    from datetime import datetime

    from pytaskq.core import auth

    action = args.action
    try:
        if action == "list":
            users = _with_redis(auth.list_users)
            if not users:
                print("No users yet. Open the dashboard and use the setup code, or run: pytaskq users create NAME --role admin")
                return
            print(f"{'USERNAME':<24}{'ROLE':<10}{'STATUS':<10}LAST SIGN-IN")
            for u in users:
                last = datetime.fromtimestamp(u.last_login_at).strftime("%Y-%m-%d %H:%M") if u.last_login_at else "never"
                print(f"{u.username:<24}{u.role:<10}{'disabled' if u.disabled else 'active':<10}{last}")
        elif action == "create":
            password = _ask_password(args.password)
            user = _with_redis(lambda r: auth.create_user(r, args.username, password, args.role, created_by="cli"))
            print(f"Created {user.role} '{user.username}'.")
        elif action == "reset-password":
            name = auth.normalize_username(args.username)
            password = _ask_password(args.password)
            _with_redis(lambda r: auth.set_password(r, name, password))
            print(f"Password for '{name}' changed; their other sessions were signed out.")
        elif action == "set-role":
            name = auth.normalize_username(args.username)
            user = _with_redis(lambda r: auth.update_user(r, name, role=args.role))
            print(f"'{user.username}' is now {user.role}.")
        elif action == "delete":
            name = auth.normalize_username(args.username)
            _with_redis(lambda r: auth.delete_user(r, name))
            print(f"Deleted '{name}'.")
        elif action == "setup-code":
            code = _with_redis(auth.ensure_setup_code)
            print(code if code else "Setup is already complete (an account exists). Use 'pytaskq users list'.")
    except auth.AuthError as e:
        raise CLIError(e.message) from e
    except CLIError:
        raise
    except Exception as e:  # connection problems etc.
        raise CLIError(f"Could not reach Redis: {e}") from e


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

    u = sub.add_parser("users", help="Manage dashboard accounts (works even when locked out)")
    us = u.add_subparsers(dest="action", required=True)
    us.add_parser("list", help="List accounts")
    c = us.add_parser("create", help="Create an account")
    c.add_argument("username")
    c.add_argument("--role", choices=["viewer", "operator", "admin"], default="viewer")
    c.add_argument("--password", default="", help="Omit to be prompted (keeps it out of shell history)")
    rp = us.add_parser("reset-password", help="Set a new password for an account")
    rp.add_argument("username")
    rp.add_argument("--password", default="")
    sr = us.add_parser("set-role", help="Change an account's role")
    sr.add_argument("username")
    sr.add_argument("role", choices=["viewer", "operator", "admin"])
    d = us.add_parser("delete", help="Delete an account")
    d.add_argument("username")
    us.add_parser("setup-code", help="Show the first-run setup code")
    u.set_defaults(func=cmd_users)
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
