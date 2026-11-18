"""test_cli.py — `pytaskq` command line helpers."""

import sys
import textwrap

import pytest

from pytaskq import cli


@pytest.fixture
def app_module(tmp_path, monkeypatch):
    (tmp_path / "myjobs.py").write_text(textwrap.dedent('''
        from pytaskq import PyTaskQ
        queue = PyTaskQ()

        @queue.task(type="io", priority="high", cron="0 * * * *")
        def hourly(report: str = "daily"):
            """Build the hourly report."""

        not_an_app = 42
    '''))
    monkeypatch.chdir(tmp_path)
    yield "myjobs"
    sys.modules.pop("myjobs", None)


def test_load_app(app_module):
    app = cli.load_app("myjobs:queue")
    assert "hourly" in app.TASKS
    assert cli.load_app("myjobs") is app, "attribute defaults to `queue`"


@pytest.mark.parametrize("spec, message", [
    ("", "No app given"),
    ("does_not_exist:queue", "Could not import"),
    ("myjobs:missing", "Could not find"),
    ("myjobs:not_an_app", "not a PyTaskQ instance"),
])
def test_load_app_errors_are_friendly(app_module, spec, message):
    with pytest.raises(cli.CLIError, match=message):
        cli.load_app(spec)


def test_tasks_command_lists_tasks(app_module, capsys):
    cli.main(["tasks", "--app", "myjobs:queue"])
    out = capsys.readouterr().out
    assert "hourly(report='daily')" in out
    assert "priority=high" in out and "cron='0 * * * *'" in out
    assert "Build the hourly report." in out
    assert "_deliver_webhook" not in out


def test_bad_app_exits_with_code_1(app_module):
    with pytest.raises(SystemExit) as exc:
        cli.main(["tasks", "--app", "nope:queue"])
    assert exc.value.code == 1


def test_worker_flags_are_applied(app_module, monkeypatch):
    import pytaskq.worker as worker
    called = {}

    async def fake_consumer():
        called["cpu"], called["io"], called["timeout"] = worker.CPU_WORKERS, worker.IO_WORKERS, worker.TASK_TIMEOUT
        called["has_task"] = "hourly" in worker.TASKS

    monkeypatch.setattr(worker, "consumer_task", fake_consumer)
    monkeypatch.setattr(worker, "CPU_WORKERS", worker.CPU_WORKERS)
    monkeypatch.setattr(worker, "IO_WORKERS", worker.IO_WORKERS)
    monkeypatch.setattr(worker, "TASK_TIMEOUT", worker.TASK_TIMEOUT)
    monkeypatch.setattr(worker, "TASKS", {})
    cli.main(["worker", "--app", "myjobs:queue", "--cpu-workers", "2", "--io-workers", "3", "--timeout", "9"])
    assert called == {"cpu": 2, "io": 3, "timeout": 9.0, "has_task": True}


def test_no_command_prints_help(capsys):
    assert cli.main([]) == 1
    assert "worker" in capsys.readouterr().out
