from __future__ import annotations

import asyncio
import sys
from io import BytesIO


class _Stdout:
    def __init__(self) -> None:
        self.buffer = BytesIO()


def test_collect_output_returns_full_text_and_mirrors_stdout(dlt_process, monkeypatch):
    stdout = _Stdout()
    monkeypatch.setattr(dlt_process.sys, "stdout", stdout)

    async def run():
        reader = asyncio.StreamReader()
        reader.feed_data(b"hello\n")
        reader.feed_data(b"world\n")
        reader.feed_eof()
        return await dlt_process.collect_output(reader)

    assert asyncio.run(run()) == "hello\nworld\n"
    assert stdout.buffer.getvalue() == b"hello\nworld\n"


def test_collect_output_keeps_rolling_tail(dlt_process, monkeypatch):
    monkeypatch.setattr(dlt_process.sys, "stdout", _Stdout())

    async def run():
        reader = asyncio.StreamReader()
        reader.feed_data(b"A" * 8000 + b"UNIQUE_TAIL")
        reader.feed_eof()
        return await dlt_process.collect_output(reader, max_keep=50)

    result = asyncio.run(run())
    assert result.endswith("UNIQUE_TAIL")
    assert len(result.encode()) < 8000


def test_stop_process_is_noop_when_already_exited(dlt_process):
    class Done:
        returncode = 0

        def kill(self):
            raise AssertionError("should not kill an exited process")

    asyncio.run(dlt_process.stop_process(Done(), "done.py"))


def test_run_pipeline_script_alerts_on_spawn_error(dlt_process, monkeypatch):
    sent: list[tuple] = []

    async def boom(*args, **kwargs):
        raise FileNotFoundError("uv")

    monkeypatch.setattr(dlt_process.asyncio, "create_subprocess_exec", boom)
    monkeypatch.setattr(
        dlt_process.alerts,
        "alert_job_error",
        lambda script_name, exc, output="": sent.append((script_name, type(exc).__name__, output)),
    )

    asyncio.run(dlt_process.run_pipeline_script("beefy_api_pipeline.py"))

    assert sent == [("beefy_api_pipeline.py", "FileNotFoundError", "")]


def test_run_pipeline_script_passes_output_on_failure(dlt_process, tmp_path, monkeypatch):
    script = tmp_path / "beefy_db_pipeline.py"
    script.write_text(
        "import sys\n"
        "print('Traceback (most recent call last):', file=sys.stderr)\n"
        "print('RuntimeError: boom', file=sys.stderr)\n"
        "sys.exit(1)\n"
    )
    sent: list[tuple] = []
    real_exec = asyncio.create_subprocess_exec

    async def collect(stream, **kwargs):
        return (await stream.read()).decode()

    async def spawn(*args, **kwargs):
        return await real_exec(
            sys.executable,
            str(script),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1024 * 1024,
        )

    monkeypatch.setattr(dlt_process, "DLT_DIR", tmp_path)
    monkeypatch.setattr(dlt_process, "collect_output", collect)
    monkeypatch.setattr(dlt_process.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(dlt_process.alerts, "alert_job_failed", lambda *args: sent.append(args))

    asyncio.run(dlt_process.run_pipeline_script("beefy_db_pipeline.py"))

    assert len(sent) == 1
    script_name, returncode, output = sent[0]
    assert script_name == "beefy_db_pipeline.py"
    assert returncode == 1
    assert "RuntimeError: boom" in output


def test_run_pipeline_script_uses_custom_timeout(dlt_process, tmp_path, monkeypatch):
    script = tmp_path / "slow_job.py"
    script.write_text("import time\ntime.sleep(60)\n")
    sent: list[int] = []
    real_exec = asyncio.create_subprocess_exec

    async def collect(stream, **kwargs):
        return (await stream.read()).decode()

    async def spawn(*args, **kwargs):
        return await real_exec(
            sys.executable,
            str(script),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1024 * 1024,
        )

    monkeypatch.setattr(dlt_process, "DLT_DIR", tmp_path)
    monkeypatch.setattr(dlt_process, "collect_output", collect)
    monkeypatch.setattr(dlt_process.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(dlt_process.alerts, "alert_job_timeout", lambda name, timeout, output="": sent.append(timeout))

    asyncio.run(dlt_process.run_pipeline_script("slow_job.py", timeout=1))

    assert sent == [1]



def test_pipeline_lock_name_is_the_pipeline_of_the_script(dlt_process):
    assert dlt_process.pipeline_lock_name("databarn_v1_pipeline.py") == "databarn_v1"
    assert dlt_process.pipeline_lock_name("beefy_clm_subgraph_pipeline.py") == "beefy_clm_subgraph"


def test_pipeline_lock_name_is_none_for_scripts_without_one_pipeline_dir(dlt_process):
    assert dlt_process.pipeline_lock_name("all_pipeline.py") is None
    assert dlt_process.pipeline_lock_name("optimize_replacing_tables.py") is None
    assert dlt_process.pipeline_lock_name("cleanup_pipeline_state.py") is None


def test_pipeline_command_runs_pipelines_under_the_lock(dlt_process):
    assert dlt_process.pipeline_command("databarn_v1_pipeline.py") == [
        dlt_process.LOCK_SCRIPT,
        "databarn_v1",
        "uv",
        "run",
        "./databarn_v1_pipeline.py",
    ]
    assert dlt_process.pipeline_command("optimize_replacing_tables.py") == [
        "uv",
        "run",
        "./optimize_replacing_tables.py",
    ]


def test_run_pipeline_script_alerts_lock_busy_apart_from_failure(dlt_process, tmp_path, monkeypatch):
    script = tmp_path / "beefy_db_pipeline.py"
    script.write_text(f"import sys\nsys.exit({dlt_process.LOCK_BUSY_EXIT_CODE})\n")
    busy: list[tuple] = []
    failed: list[tuple] = []
    real_exec = asyncio.create_subprocess_exec

    async def spawn(*args, **kwargs):
        return await real_exec(
            sys.executable,
            str(script),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1024 * 1024,
        )

    monkeypatch.setattr(dlt_process.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(dlt_process.alerts, "alert_job_lock_busy", lambda *args: busy.append(args))
    monkeypatch.setattr(dlt_process.alerts, "alert_job_failed", lambda *args: failed.append(args))

    asyncio.run(dlt_process.run_pipeline_script("beefy_db_pipeline.py"))

    assert busy == [("beefy_db_pipeline.py", dlt_process.LOCK_WAIT_SECONDS)]
    assert failed == []
