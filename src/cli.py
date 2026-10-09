import argparse
import importlib
import sys
import asyncio
import os
import logging
from src.worker import set_tasks, consumer_task
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="PyTaskQ CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # worker command
    worker_parser = subparsers.add_parser("worker", help="Start a PyTaskQ worker")
    worker_parser.add_argument("--app", required=True, help="App instance to use (e.g. main:queue)")

    args = parser.parse_args()

    if args.command == "worker":
        # Add current directory to path so dynamic imports work for the user
        sys.path.insert(0, os.getcwd())

        # Parse --app
        try:
            module_name, app_name = args.app.split(":")
        except ValueError:
            logging.error("Invalid --app format. Use module:app_name (e.g. main:queue)")
            sys.exit(1)

        try:
            module = importlib.import_module(module_name)
            queue_app = getattr(module, app_name)
        except ImportError as e:
            logging.error(f"Could not import module '{module_name}': {e}")
            sys.exit(1)
        except AttributeError:
            logging.error(f"Could not find app '{app_name}' in module '{module_name}'")
            sys.exit(1)

        if not hasattr(queue_app, "TASKS"):
            logging.error(f"Provided app '{app_name}' does not appear to be a valid PyTaskQ instance.")
            sys.exit(1)

        # Pass tasks to worker
        set_tasks(queue_app.TASKS)
        
        # Start worker
        logging.info(f"Starting PyTaskQ worker for app: {args.app}")
        asyncio.run(consumer_task())
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
