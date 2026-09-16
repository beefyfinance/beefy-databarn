"""Discord webhook notifier with once-per-UTC-day dedup.

Never logs the webhook URL. No-op when DISCORD_WEBHOOK_URL is unset.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DISCORD_DESCRIPTION_LIMIT = 4096
DISCORD_TITLE_LIMIT = 256


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _load_state(state_path: Path) -> dict[str, Any]:
    try:
        return json.loads(state_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.replace(state_path)


def notify_once_per_day(
    key: str,
    title: str,
    description: str,
    state_path: str | Path,
    *,
    color: int = 0xE74C3C,
) -> bool:
    """POST a Discord embed at most once per UTC day per key. Returns True if sent."""
    url = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        logger.debug("DISCORD_WEBHOOK_URL unset; skipping Discord notify for %s", key)
        return False

    path = Path(state_path)
    state = _load_state(path)
    if state.get(key) == _today_utc():
        logger.debug("Discord alert %s already sent today; skipping", key)
        return False

    payload = {
        "username": "databarn",
        "embeds": [
            {
                "title": title[:DISCORD_TITLE_LIMIT],
                "description": description[:DISCORD_DESCRIPTION_LIMIT],
                "color": color,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ],
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "beefy-databarn",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status not in (200, 204):
                logger.error("Discord webhook returned HTTP %s", response.status)
                return False
    except urllib.error.URLError as exc:
        logger.error("Discord webhook request failed: %s", exc)
        return False
    except Exception:
        logger.exception("Discord webhook request failed")
        return False

    state[key] = _today_utc()
    try:
        _save_state(path, state)
    except OSError:
        logger.exception("Failed to persist Discord alert state")
    logger.info("Discord alert sent for %s", key)
    return True
