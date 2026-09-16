"""Rewind an incremental resource and re-append from a start date.

Destination rows are never truncated or deleted. Windowed beefy_db tables
use ReplacingMergeTree; staging reads them with FINAL so duplicates collapse.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import dlt
import pendulum
from dlt.pipeline.exceptions import PipelineHasPendingDataException, PipelineNeverRan
from dlt.pipeline.helpers import pipeline_drop

logger = logging.getLogger(__name__)

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RELATIVE = re.compile(r"^(\d+)\s*(d|w|mo|m|y)$", re.IGNORECASE)


def parse_since(value: str) -> Optional[datetime]:
    """Parse ``full``, ``3m`` / ``90d``, or an ISO date into a UTC datetime.

    Relative values are truncated to 00:00 UTC. ``full`` / ``all`` means the
    resource's own genesis cursor (return None).
    """
    value = value.strip()
    if value.lower() in {"full", "all"}:
        return None

    relative = _RELATIVE.fullmatch(value)
    if relative:
        amount = int(relative.group(1))
        unit = relative.group(2).lower()
        now = pendulum.now("UTC")
        if unit == "d":
            parsed = now.subtract(days=amount)
        elif unit == "w":
            parsed = now.subtract(weeks=amount)
        elif unit in {"m", "mo"}:
            parsed = now.subtract(months=amount)
        else:
            parsed = now.subtract(years=amount)
        parsed = parsed.start_of("day")
        return datetime(
            parsed.year,
            parsed.month,
            parsed.day,
            tzinfo=timezone.utc,
        )

    try:
        parsed = pendulum.parse(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Invalid --reimport value {value!r}. Use YYYY-MM-DD, 90d, 3m, or omit for full history."
        ) from exc
    if parsed is None:
        raise ValueError(
            f"Invalid --reimport value {value!r}. Use YYYY-MM-DD, 90d, 3m, or omit for full history."
        )
    if isinstance(parsed, pendulum.Date) and not isinstance(parsed, pendulum.DateTime):
        parsed = pendulum.datetime(parsed.year, parsed.month, parsed.day, tz="UTC")
    if not isinstance(parsed, datetime):
        raise ValueError(
            f"Invalid --reimport value {value!r}. Use YYYY-MM-DD, 90d, 3m, or omit for full history."
        )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.in_timezone("UTC")
    return datetime(
        parsed.year,
        parsed.month,
        parsed.day,
        parsed.hour,
        parsed.minute,
        parsed.second,
        parsed.microsecond,
        tzinfo=timezone.utc,
    )


def _cursor_column(resource: Any) -> str:
    incremental = resource.incremental
    if not incremental:
        raise ValueError(
            f"Resource {resource.name} is not incremental; --reimport only works on incremental resources"
        )
    cursor = getattr(incremental, "cursor_path", None)
    if not cursor or not _IDENT.fullmatch(str(cursor)):
        raise ValueError(
            f"Resource {resource.name} cursor {cursor!r} is not a simple column; cannot reimport a date range"
        )
    return str(cursor)


def _rewind_incremental(resource: Any, since: datetime) -> None:
    incremental = resource.incremental
    new_inc = dlt.sources.incremental(
        incremental.cursor_path,
        initial_value=since,
        primary_key=incremental.primary_key,
        last_value_func=incremental.last_value_func,
        row_order=getattr(incremental, "row_order", "asc"),
    )
    threshold = incremental.__dict__.get("duplicate_cursor_warning_threshold")
    if threshold is not None:
        new_inc.duplicate_cursor_warning_threshold = threshold
    resource.apply_hints(incremental=new_inc)
    resource.incremental.initial_value = since


def _drop_resource_state(pipeline: dlt.Pipeline, resource_name: str) -> None:
    try:
        if pipeline.has_pending_data:
            logger.warning("Dropping pending packages before reimport")
            pipeline.drop_pending_packages()
        pipeline.sync_destination()
        dropper = pipeline_drop(pipeline, resources=[resource_name], state_only=True)
        print(f"Dropping incremental state for {resource_name}: {dropper.info}")
        dropper()
    except PipelineNeverRan:
        logger.info("Pipeline has never run; skip state drop")
    except PipelineHasPendingDataException:
        pipeline.drop_pending_packages()
        dropper = pipeline_drop(pipeline, resources=[resource_name], state_only=True)
        dropper()


def prepare_reimport(
    pipeline: dlt.Pipeline, source: Any, resource_name: str, since_spec: str
) -> None:
    """Drop resource incremental state and rewind the cursor. Never truncates data."""
    if resource_name not in source.resources:
        raise ValueError(f"Resource {resource_name} not found in source {source.name}")

    resource = source.resources[resource_name]
    cursor = _cursor_column(resource)
    since = parse_since(since_spec)

    print(
        f"Reimport {pipeline.pipeline_name}.{resource_name} "
        f"from {since.isoformat() if since else 'full history'} "
        f"(cursor {cursor}). Existing ClickHouse rows are kept; "
        "ReplacingMergeTree collapses duplicates on merge."
    )

    _drop_resource_state(pipeline, resource_name)
    if since is not None:
        _rewind_incremental(resource, since)
        print(f"Incremental initial_value set to {since.isoformat()}")
