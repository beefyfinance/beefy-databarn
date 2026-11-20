"""Subprocess helpers for the dlt scheduler."""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

import alerts

logger = logging.getLogger(__name__)

TASK_TIMEOUT = 60 * 30  # 30 minutes timeout max
DLT_DIR = Path("/app/dlt")
OUTPUT_TAIL_BYTES = 64 * 1024

# Pipelines run under a per-pipeline lock (run_with_lock.sh) so a manual `make dlt loop` or
# `reimport` in the same container never shares a working directory with a scheduled run.
LOCK_SCRIPT = "/app/infra/dlt/run_with_lock.sh"
# run_with_lock.sh exits with this when another run held the lock past the wait timeout.
# Keep in sync with LOCK_BUSY_EXIT_CODE in run_with_lock.sh.
LOCK_BUSY_EXIT_CODE = 75
# Scheduled runs skip rather than queue up behind a long manual run.
LOCK_WAIT_SECONDS = 30


def pipeline_lock_name(script_name: str) -> str | None:
    """The pipeline whose working directory a script uses: `databarn_v1_pipeline.py` -> `databarn_v1`.

    None for scripts that use no pipeline directory (optimize, state cleanup) or several (all).
    """
    suffix = "_pipeline.py"
    if script_name == f"all{suffix}" or not script_name.endswith(suffix):
        return None
    return script_name[: -len(suffix)]


def pipeline_command(script_name: str) -> list[str]:
    command = ["uv", "run", f"./{script_name}"]
    name = pipeline_lock_name(script_name)
    return command if name is None else [LOCK_SCRIPT, name, *command]


async def collect_output(
    stream: asyncio.StreamReader,
    *,
    max_keep: int = OUTPUT_TAIL_BYTES,
) -> str:
    """Copy subprocess output to the console and keep a rolling tail for Discord."""
    chunks: list[bytes] = []
    size = 0
    while True:
        data = await stream.read(4096)
        if not data:
            break
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
        chunks.append(data)
        size += len(data)
        while size > max_keep and len(chunks) > 1:
            size -= len(chunks.pop(0))
    return b"".join(chunks).decode("utf-8", errors="replace")


async def stop_process(process: asyncio.subprocess.Process, name: str) -> None:
    """Kill a still-running subprocess and wait for it to exit."""
    if process.returncode is not None:
        return
    logger.warning(f"Killing {name} process...")
    try:
        process.kill()
        await asyncio.wait_for(process.wait(), timeout=30)
    except asyncio.TimeoutError:
        logger.error(f"Process {name} did not terminate after kill signal")


async def run_pipeline_script(script_name: str, timeout: int = TASK_TIMEOUT) -> None:
    """Run a pipeline script using uv run."""
    logger.info(f"Starting {script_name} pipeline run...")
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["DLT_RUN_LOCK_TIMEOUT"] = str(LOCK_WAIT_SECONDS)
    try:
        process = await asyncio.create_subprocess_exec(
            *pipeline_command(script_name),
            cwd=str(DLT_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
            limit=1024 * 1024,
        )
    except Exception as e:
        logger.error(f"Error running {script_name}: {e}", exc_info=True)
        alerts.alert_job_error(script_name, e)
        return

    assert process.stdout is not None
    collector = asyncio.create_task(collect_output(process.stdout))
    timed_out = False
    error: BaseException | None = None
    output = ""
    try:
        async with asyncio.timeout(timeout):
            await process.wait()
    except TimeoutError:
        timed_out = True
        logger.error(f"{script_name} timed out after {timeout}s")
    except Exception as e:
        error = e
        logger.error(f"Error running {script_name}: {e}", exc_info=True)
    finally:
        await stop_process(process, script_name)
        try:
            output = await asyncio.wait_for(collector, timeout=10)
        except Exception:
            logger.exception(f"Failed to collect output from {script_name}")

    if timed_out:
        alerts.alert_job_timeout(script_name, timeout, output)
        return
    if error is not None:
        alerts.alert_job_error(script_name, error, output)
        return
    if process.returncode == 0:
        logger.info(f"{script_name} pipeline run completed successfully")
        return

    if process.returncode == LOCK_BUSY_EXIT_CODE:
        logger.warning(f"{script_name} skipped: another run held the pipeline lock")
        alerts.alert_job_lock_busy(script_name, LOCK_WAIT_SECONDS)
        return

    logger.error(f"{script_name} pipeline failed with return code {process.returncode}")
    alerts.alert_job_failed(script_name, process.returncode, output)
