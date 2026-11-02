"""Keyset paging over the export API of databarn v1 (beefy-bi)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional

import pyarrow as pa

from lib.config import get_databarn_v1_api
from lib.fetch import RequestPacer, fetch_url_json_dict_with_params
from .endpoints import DECIMAL, TIMESTAMP, TimeSeries

PAGE_SIZE = 10_000
# Reads the production databarn v1 database; the API itself allows far more.
MIN_REQUEST_INTERVAL_S = 0.2
# The gateway answers 504 after 60s.
_TIMEOUT_S = 90.0

_pacer = RequestPacer(MIN_REQUEST_INTERVAL_S)


def _format_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


async def iter_pages(
    path: str, params: Optional[Mapping[str, str]] = None
) -> AsyncIterator[List[Dict[str, Any]]]:
    """Yield each page of an export, following next_cursor with the same params."""
    base_url, api_key = get_databarn_v1_api()
    cursor: Optional[str] = None
    while True:
        query: Dict[str, Any] = {**(params or {}), "limit": PAGE_SIZE}
        if cursor is not None:
            query["cursor"] = cursor
        await _pacer.wait()
        payload, _ = await fetch_url_json_dict_with_params(
            f"{base_url}/export/{path}",
            params=query,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout_s=_TIMEOUT_S,
        )
        yield payload["data"]
        cursor = payload["next_cursor"]
        if cursor is None:
            return


def _trim_decimal(value: Optional[str]) -> Optional[str]:
    if value is None or "." not in value:
        return value
    return value[: value.index(".") + 1 + DECIMAL.scale]


def _column(values: List[Any], data_type: pa.DataType) -> pa.Array:
    if data_type == DECIMAL:
        return pa.array([_trim_decimal(v) for v in values], pa.string()).cast(DECIMAL)
    if data_type == TIMESTAMP:
        return pa.array(values, pa.string()).cast(TIMESTAMP)
    if data_type == pa.float64():
        # JSON whole numbers parse to int, and pyarrow refuses those above 2^53.
        return pa.array([None if v is None else float(v) for v in values], data_type)
    return pa.array(values, data_type)


def rows_to_arrow(endpoint: TimeSeries, rows: List[Dict[str, Any]]) -> pa.Table:
    fields = [
        pa.field(name, data_type, nullable=name not in endpoint.not_null)
        for name, data_type in endpoint.columns.items()
    ]
    arrays = [_column([row[field.name] for row in rows], field.type) for field in fields]
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


async def iter_time_series(
    endpoint: TimeSeries, start: datetime, end: datetime
) -> AsyncIterator[pa.Table]:
    """Read [start, end) in time order, stopping at the page that crosses the row cap.

    Rows come back ordered by datetime, so the next run resumes from the last one and only
    re-reads the rows sharing that datetime. Those collapse in ReplacingMergeTree. Empty
    windows do not count: there is no row to resume from, so a gap has to be crossed.
    """
    rows_read = 0
    window_start = start
    while window_start < end:
        window_end = min(window_start + endpoint.window, end)
        params = {"from": _format_datetime(window_start), "to": _format_datetime(window_end)}
        async for rows in iter_pages(endpoint.path, params):
            if rows:
                yield rows_to_arrow(endpoint, rows)
                rows_read += len(rows)
            if rows_read >= endpoint.max_rows_per_run:
                return
        window_start = window_end
