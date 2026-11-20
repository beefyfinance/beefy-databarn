from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "dlt" / "run_with_lock.sh"

pytestmark = pytest.mark.skipif(shutil.which("flock") is None, reason="needs util-linux flock")


def _run(lock_dir: Path, name: str, *command: str, timeout: str = "1") -> subprocess.Popen:
    env = {**os.environ, "DLT_RUN_LOCK_DIR": str(lock_dir), "DLT_RUN_LOCK_TIMEOUT": timeout}
    return subprocess.Popen(
        [str(SCRIPT), name, *command],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_runs_the_command_and_returns_its_exit_code(tmp_path):
    ok = _run(tmp_path, "p", sys.executable, "-c", "print('ran')")
    assert ok.communicate()[0].strip() == "ran"
    assert ok.returncode == 0

    failing = _run(tmp_path, "p", sys.executable, "-c", "raise SystemExit(3)")
    failing.communicate()
    assert failing.returncode == 3


def test_second_run_of_same_pipeline_gives_up_with_lock_busy_code(tmp_path):
    holder = _run(tmp_path, "p", sys.executable, "-c", "import time; time.sleep(30)")
    try:
        time.sleep(0.5)
        second = _run(tmp_path, "p", sys.executable, "-c", "print('should not run')")
        out, err = second.communicate(timeout=15)
        assert second.returncode == 75
        assert "should not run" not in out
        assert "holds the p lock" in err
    finally:
        holder.kill()
        holder.communicate()


def test_other_pipelines_do_not_wait_for_each_other(tmp_path):
    holder = _run(tmp_path, "a", sys.executable, "-c", "import time; time.sleep(30)")
    try:
        time.sleep(0.5)
        other = _run(tmp_path, "b", sys.executable, "-c", "print('ok')")
        assert other.communicate(timeout=15)[0].strip() == "ok"
        assert other.returncode == 0
    finally:
        holder.kill()
        holder.communicate()


def test_lock_is_released_when_the_holder_dies(tmp_path):
    holder = _run(tmp_path, "p", sys.executable, "-c", "import time; time.sleep(30)")
    time.sleep(0.5)
    holder.kill()
    holder.communicate()

    again = _run(tmp_path, "p", sys.executable, "-c", "print('ok')")
    assert again.communicate(timeout=15)[0].strip() == "ok"
    assert again.returncode == 0


def test_waiting_run_starts_once_the_holder_finishes(tmp_path):
    holder = _run(tmp_path, "p", sys.executable, "-c", "import time; time.sleep(1.5)")
    time.sleep(0.5)
    waiting = _run(tmp_path, "p", sys.executable, "-c", "print('ok')", timeout="20")
    assert waiting.communicate(timeout=25)[0].strip() == "ok"
    assert waiting.returncode == 0
    holder.communicate()


def test_usage_error_without_a_command(tmp_path):
    result = subprocess.run([str(SCRIPT), "p"], capture_output=True, text=True)
    assert result.returncode == 2
