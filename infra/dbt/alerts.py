"""Discord alerts for dbt run and test failures."""
from __future__ import annotations

import json
import re
from pathlib import Path

from discord_webhook import as_code_block, as_inline_code, notify_once_per_day

DBT_DIR = Path("/app/dbt")
RUN_RESULTS_PATH = DBT_DIR / "target" / "run_results.json"
ALERT_STATE_PATH = DBT_DIR / "target" / "discord_alert_state.json"

MAX_ITEMS = 10
FAIL_STATUSES = {"fail", "error"}
_HEX32 = re.compile(r"^[0-9a-f]{32}$")
_COMPILED_SQL_RE = re.compile(r"^\s*compiled(\s+code\s+at|\s+sql)\b", re.IGNORECASE)


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


def _compact_dbt_message(message: str) -> str:
    """Keep the actual error; drop trailing compiled-SQL dumps."""
    lines: list[str] = []
    for raw in message.splitlines():
        if _COMPILED_SQL_RE.match(raw):
            break
        lines.append(raw.rstrip())
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines).strip()


def _format_failed_result(result: dict, *, message_chars: int) -> str:
    unique_id = result.get("unique_id") or "unknown"
    name = as_inline_code(_display_name(unique_id))
    failures = result.get("failures")
    header = f"• {name}"
    if failures is not None:
        header += f" ({failures} rows)"

    message = _compact_dbt_message(result.get("message") or "")
    if not message:
        return header
    return f"{header}\n{as_code_block(message, max_chars=message_chars, keep="start")}"


def _summarize_run_results(path: Path, *, max_items: int = MAX_ITEMS) -> str:
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return "Could not read dbt run_results.json."

    failed_results = [
        result
        for result in data.get("results", [])
        if str(result.get("status", "")).lower() in FAIL_STATUSES
    ]
    total = len(failed_results)
    if total == 0:
        return "dbt reported a non-zero exit but no failed nodes in run_results.json."

    shown_results = failed_results[:max_items]
    message_chars = min(1500, max(250, 3200 // max(1, len(shown_results))))
    shown = [_format_failed_result(result, message_chars=message_chars) for result in shown_results]
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
