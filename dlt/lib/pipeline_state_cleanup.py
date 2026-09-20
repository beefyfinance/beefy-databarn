"""Conservative cleanup of historical dlt `_dlt_pipeline_state` rows.

dlt appends a full compressed snapshot on every run. Per `pipeline_name`,
delete rows older than the retention window, but never drop below a minimum
number of newest rows.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

STATE_TABLE_SUFFIX = "____dlt_pipeline_state"
DEFAULT_RETENTION_DAYS = 30
DEFAULT_KEEP_ROWS = 50


def retention_days() -> int:
    return max(1, int(os.environ.get("DLT_PIPELINE_STATE_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS))))


def keep_rows() -> int:
    return max(1, int(os.environ.get("DLT_PIPELINE_STATE_KEEP_ROWS", str(DEFAULT_KEEP_ROWS))))


def obsolete_state_delete_sql(ident: str, days: int, min_keep: int) -> str:
    """One mutation: drop rows older than `days`, keeping `min_keep` newest per pipeline_name."""
    days_n = int(days)
    keep_n = int(min_keep)
    return (
        f"ALTER TABLE {ident} DELETE WHERE "
        f"created_at < now() - INTERVAL {days_n} DAY "
        f"AND (pipeline_name, _dlt_load_id) NOT IN ("
        f"SELECT pipeline_name, _dlt_load_id FROM {ident} "
        f"ORDER BY created_at DESC, _dlt_load_id DESC "
        f"LIMIT {keep_n} BY pipeline_name"
        f") SETTINGS mutations_sync = 1"
    )


async def cleanup_dlt_pipeline_state() -> None:
    from lib.clickhouse import _ident, clickhouse_default_database, get_clickhouse_client

    client = await get_clickhouse_client()
    database = clickhouse_default_database()
    days = retention_days()
    min_keep = keep_rows()
    tables = await client.query(
        """
        SELECT name
        FROM system.tables
        WHERE database = %(database)s
          AND endsWith(name, %(suffix)s)
        ORDER BY name
        """,
        parameters={"database": database, "suffix": STATE_TABLE_SUFFIX},
    )
    for (full_name,) in tables.result_rows:
        ident = _ident(database, str(full_name))
        sql = obsolete_state_delete_sql(ident, days, min_keep)
        logger.info(
            "Cleaning %s (retention %sd, min %s rows per pipeline_name)",
            ident,
            days,
            min_keep,
        )
        await client.command(sql)
