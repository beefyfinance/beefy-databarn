"""Discord alerts for dbt run and test failures."""
from __future__ import annotations

import json
import re
from pathlib import Path

from discord_webhook import notify_once_per_day

DBT_DIR = Path("/app/dbt")
RUN_RESULTS_PATH = DBT_DIR / "target" / "run_results.json"
ALERT_STATE_PATH = DBT_DIR / "target" / "discord_alert_state.json"

MAX_ITEMS = 15
FAIL_STATUSES = {"fail", "error"}
_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def _alert(key: str, title: str, description: str) -> None:
    notify_once_per_day(key, title, description, ALERT_STATE_PATH)


def _display_name(unique_id: str) -> str:
    parts = unique_id.split(".")
    if len(parts) >= 3:
        name_parts = parts[2:]
        if len(name_parts) > 1 and _HEX32.match(name_parts[-1]):
            name_parts = name_parts[:-1]
        return ".".join(name_parts)
    return unique_id


def _summarize_run_results(path: Path, *, max_items: int = MAX_ITEMS) -> str:
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return "Could not read dbt run_results.json."

    failed: list[str] = []
    for result in data.get("results", []):
        if str(result.get("status", "")).lower() not in FAIL_STATUSES:
            continue
        unique_id = result.get("unique_id") or "unknown"
        name = _display_name(unique_id)
        failures = result.get("failures")
        if failures is not None:
            failed.append(f"• {name} ({failures} rows)")
            continue
        message = (result.get("message") or "").strip().splitlines()
        extra = f" — {message[0][:120]}" if message else ""
        failed.append(f"• {name}{extra}")

    total = len(failed)
    if total == 0:
        return "dbt reported a non-zero exit but no failed nodes in run_results.json."

    shown = failed[:max_items]
    text = f"{total} failed\n" + "\n".join(shown)
    remaining = total - len(shown)
    if remaining > 0:
        text += f"\n… and {remaining} more"
    return text


def _results_written_since(started: float) -> bool:
    try:
        return RUN_RESULTS_PATH.stat().st_mtime >= started - 1
    except OSError:
        return False


def _failure_description(started: float, returncode: int, kind: str) -> str:
    if _results_written_since(started):
        return _summarize_run_results(RUN_RESULTS_PATH)
    return (
        f"dbt {kind} failed with exit code {returncode} and did not write run_results.json. "
        "This is often a lock timeout or an infra error before dbt started."
    )


def alert_deps_failed(key: str, returncode: int) -> None:
    _alert(
        key,
        "dbt deps failed (prod)",
        f"dbt deps exited with return code {returncode}.",
    )


def alert_run_failed(started: float, returncode: int) -> None:
    _alert("dbt_run", "dbt run failed (prod)", _failure_description(started, returncode, "run"))


def alert_run_exception(exc: BaseException) -> None:
    _alert("dbt_run", "dbt run failed (prod)", f"dbt run raised {type(exc).__name__}: {exc}")


def alert_test_failed(started: float, returncode: int) -> None:
    if _results_written_since(started):
        _alert("dbt_test", "dbt tests failed (prod)", _summarize_run_results(RUN_RESULTS_PATH))
        return
    _alert(
        "dbt_test_lock",
        "dbt test lock/infra error (prod)",
        _failure_description(started, returncode, "test"),
    )


def alert_test_exception(exc: BaseException) -> None:
    _alert(
        "dbt_test_lock",
        "dbt test lock/infra error (prod)",
        f"dbt test raised {type(exc).__name__}: {exc}",
    )
