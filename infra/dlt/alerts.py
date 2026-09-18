"""Discord alerts for dlt pipeline and scheduler failures."""
from __future__ import annotations

import re
from pathlib import Path

from discord_webhook import as_code_block, as_inline_code, notify_once_per_day

ALERT_STATE_PATH = Path("/var/dlt/discord_alert_state.json")

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_ERROR_MARKERS = (
    "Traceback (most recent call last):",
    "Pipeline execution failed",
)
_ERROR_LINE_RE = re.compile(
    r"\[ERROR\]|\bERROR\b|\bCRITICAL\b|[A-Za-z_]*Exception:|[A-Za-z_]*Error:"
)
# First ": " after the URL. Ports (":443") have no space, so they stay in the URL.
_FETCH_FAIL_RE = re.compile(r"Failed to reach (\S+?): ([^\n]+)")


def _alert(key: str, title: str, description: str) -> None:
    notify_once_per_day(key, title, description, ALERT_STATE_PATH)


def _log_excerpt(output: str, *, max_chars: int = 1800) -> str:
    """Keep the traceback or last error lines from a failed pipeline run."""
    text = _ANSI_RE.sub("", output.replace("\r\n", "\n").replace("\r", "\n")).strip()
    if not text:
        return ""

    start = max((text.rfind(marker) for marker in _ERROR_MARKERS), default=-1)
    if start >= 0:
        text = text[start:]
    else:
        lines = text.splitlines()
        error_line = next(
            (i for i in range(len(lines) - 1, -1, -1) if _looks_like_error(lines[i])),
            None,
        )
        if error_line is not None:
            text = "\n".join(lines[max(0, error_line - 5) :])
        else:
            text = "\n".join(lines[-40:])
    # Exception lines sit at the end. Keep the tail so Discord does not show
    # the middle of a long httpx stack and drop the actual error.
    limit = max_chars * 2
    text = text.strip()
    if len(text) > limit:
        text = text[-limit:]
    return text.strip()


def _looks_like_error(line: str) -> bool:
    return _ERROR_LINE_RE.search(line) is not None


def _fetch_failure_lines(output: str) -> list[str]:
    text = _ANSI_RE.sub("", output.replace("\r\n", "\n").replace("\r", "\n"))
    lines: list[str] = []
    for url, detail in _FETCH_FAIL_RE.findall(text):
        line = f"Failed to reach {as_inline_code(url)}: {detail.strip()}"
        if line not in lines:
            lines.append(line)
    return lines[:5]


def _with_output(prefix: str, output: str = "") -> str:
    failures = _fetch_failure_lines(output)
    if failures:
        prefix = prefix + "\n" + "\n".join(failures)
    excerpt = _log_excerpt(output)
    if not excerpt:
        return prefix
    return f"{prefix}\n\n{as_code_block(excerpt)}"


def alert_job_failed(script_name: str, returncode: int, output: str = "") -> None:
    _alert(
        script_name,
        f"dlt job failed: {script_name}",
        _with_output(
            f"Pipeline {as_inline_code(script_name)} exited with return code {returncode}.",
            output,
        ),
    )


def alert_job_timeout(script_name: str, timeout_seconds: int, output: str = "") -> None:
    _alert(
        script_name,
        f"dlt job timed out: {script_name}",
        _with_output(
            f"Pipeline {as_inline_code(script_name)} exceeded the {timeout_seconds // 60}-minute timeout.",
            output,
        ),
    )


def alert_job_error(script_name: str, exc: BaseException, output: str = "") -> None:
    _alert(
        script_name,
        f"dlt job error: {script_name}",
        _with_output(
            f"Pipeline {as_inline_code(script_name)} raised {type(exc).__name__}: {exc}",
            output,
        ),
    )


def alert_scheduler_crash(exc: BaseException) -> None:
    _alert(
        "dlt_scheduler",
        "dlt scheduler crashed",
        f"Scheduler raised {type(exc).__name__}: {exc}",
    )
