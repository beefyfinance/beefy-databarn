"""Discord alerts for dlt pipeline and scheduler failures."""
from __future__ import annotations

from pathlib import Path

from discord_webhook import notify_once_per_day

ALERT_STATE_PATH = Path("/var/dlt/discord_alert_state.json")


def _alert(key: str, title: str, description: str) -> None:
    notify_once_per_day(key, title, description, ALERT_STATE_PATH)


def alert_job_failed(script_name: str, returncode: int) -> None:
    _alert(
        script_name,
        f"dlt job failed: {script_name}",
        f"Pipeline `{script_name}` exited with return code {returncode}.",
    )


def alert_job_timeout(script_name: str, timeout_seconds: int) -> None:
    _alert(
        script_name,
        f"dlt job timed out: {script_name}",
        f"Pipeline `{script_name}` exceeded the {timeout_seconds // 60}-minute timeout.",
    )


def alert_job_error(script_name: str, exc: BaseException) -> None:
    _alert(
        script_name,
        f"dlt job error: {script_name}",
        f"Pipeline `{script_name}` raised {type(exc).__name__}: {exc}",
    )


def alert_scheduler_crash(exc: BaseException) -> None:
    _alert(
        "dlt_scheduler",
        "dlt scheduler crashed",
        f"Scheduler raised {type(exc).__name__}: {exc}",
    )
