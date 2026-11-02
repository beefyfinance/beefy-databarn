from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Optional

import dlt
import pyarrow as pa
from dlt.destinations.adapters import clickhouse_adapter

from lib.clickhouse import get_clickhouse_client
from .resources.databarn_v1.client import iter_pages, iter_time_series
from .resources.databarn_v1.endpoints import (
    DECIMAL,
    DIMENSIONS,
    TIME_SERIES,
    TIMESTAMP,
    Dimension,
    TimeSeries,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "databarn_v1"


def _clickhouse_type(data_type: pa.DataType) -> str:
    if data_type == DECIMAL:
        return f"Decimal({DECIMAL.precision}, {DECIMAL.scale})"
    if data_type == TIMESTAMP:
        return "DateTime64(6, 'UTC')"
    if data_type == pa.int64():
        return "Int64"
    if data_type == pa.float64():
        return "Float64"
    return "String"


def table_sql(endpoint: TimeSeries) -> str:
    """Partitioned, so the daily OPTIMIZE FINAL only rewrites the newest month."""
    columns = []
    for name, data_type in endpoint.columns.items():
        column_type = _clickhouse_type(data_type)
        if name not in endpoint.not_null:
            column_type = f"Nullable({column_type})"
        columns.append(f"`{name}` {column_type} CODEC(ZSTD(3))")
    body = ",\n        ".join(columns)
    sort = ", ".join(endpoint.sort)
    return f"""
    CREATE TABLE IF NOT EXISTS {SOURCE_NAME}___{endpoint.table}
    (
        {body}
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toYYYYMM(datetime)
    ORDER BY ({sort})
    SETTINGS index_granularity = 8192;
    """


def start_datetime(
    endpoint: TimeSeries, lagged_value: Optional[datetime], now: datetime
) -> datetime:
    """Rewind only near the head: older rows are final, and rewinding on every run of a
    backfill would re-read the lag each time."""
    head = now - endpoint.lag
    start = endpoint.start if lagged_value is None else min(lagged_value + endpoint.lag, head)
    if endpoint.retention is not None:
        oldest = now - endpoint.retention
        if lagged_value is not None and start < oldest:
            logger.warning("%s: rows before %s are gone from the API", endpoint.table, oldest)
        start = max(start, oldest)
    return start


def _dimension_resource(endpoint: Dimension) -> Any:
    columns: Dict[str, Dict[str, Any]] = {
        endpoint.primary_key: {"data_type": endpoint.primary_key_type, "nullable": False},
    }
    if endpoint.json_column:
        columns[endpoint.json_column] = {"data_type": "json"}

    @dlt.resource(
        name=endpoint.table,
        primary_key=[endpoint.primary_key],
        write_disposition="append",
        columns=columns,
    )
    async def full_refresh_rows() -> AsyncIterator[Any]:
        async for rows in iter_pages(endpoint.path):
            yield rows

    return clickhouse_adapter(
        full_refresh_rows(),
        table_engine_type="replacing_merge_tree",
        sort=[endpoint.primary_key],
        settings={"allow_nullable_key": False},
    )


def _time_series_resource(endpoint: TimeSeries) -> Any:
    @dlt.resource(
        name=endpoint.table,
        primary_key=list(endpoint.sort),
        write_disposition="append",
    )
    async def incremental_rows(
        # The default only marks the argument for dlt: annotations are strings in this module.
        incremental: dlt.sources.incremental[Any] = dlt.sources.incremental("datetime"),
    ) -> AsyncIterator[pa.Table]:
        now = datetime.now(timezone.utc)
        start = start_datetime(endpoint, incremental.last_value, now)
        async for table in iter_time_series(endpoint, start, now):
            yield table

    # Bound as an explicit `incremental` argument: lib.reimport rewinds it from there.
    return incremental_rows(
        incremental=dlt.sources.incremental(
            "datetime",
            initial_value=None,
            # Re-read rows keep their key and cursor value: dlt must not drop them as
            # duplicates, ReplacingMergeTree keeps the latest.
            primary_key=(),
            lag=endpoint.lag.total_seconds(),
            last_value_func=max,
            row_order="asc",
        )
    )


@dlt.source(name=SOURCE_NAME, parallelized=True, max_table_nesting=0)
async def databarn_v1_source() -> Any:
    client = await get_clickhouse_client()
    for endpoint in TIME_SERIES:
        await client.query(table_sql(endpoint))

    return [
        *(_dimension_resource(endpoint) for endpoint in DIMENSIONS),
        *(_time_series_resource(endpoint) for endpoint in TIME_SERIES),
    ]
