from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

import sources.databarn_v1 as source
from sources.resources.databarn_v1 import client
from sources.resources.databarn_v1.endpoints import DIMENSIONS, TIME_SERIES, TimeSeries

ENDPOINTS = {endpoint.table: endpoint for endpoint in TIME_SERIES}
RAW = ENDPOINTS["prices_raw"]
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _raw_row(minute: int) -> dict[str, Any]:
    return {
        "datetime": f"2026-01-01T00:{minute:02d}:00.000000Z",
        "price_feed_id": 1,
        "block_number": minute,
        "price": "1.500000000000000000000000",
    }


def _install_api(
    monkeypatch: pytest.MonkeyPatch, pages: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    async def fetch(url: str, *, params: Any, headers: Any, timeout_s: float) -> Any:
        calls.append({"url": url, "params": params, "headers": headers})
        return pages.pop(0), None

    monkeypatch.setattr(client, "fetch_url_json_dict_with_params", fetch)
    monkeypatch.setattr(client, "get_databarn_v1_api", lambda: ("https://x/api/v1", "token"))
    monkeypatch.setattr(client, "_pacer", type("P", (), {"wait": _no_wait})())
    return calls


async def _no_wait(self: Any = None) -> None:
    return None


def _collect(endpoint: TimeSeries, start: datetime, end: datetime) -> list[Any]:
    async def run() -> list[Any]:
        return [table async for table in client.iter_time_series(endpoint, start, end)]

    return asyncio.run(run())


def test_investor_counts_is_not_ingested() -> None:
    paths = {e.path for e in TIME_SERIES} | {e.path for e in DIMENSIONS}
    assert "investor-counts" not in paths


def test_import_states_are_a_dimension_keyed_by_a_string() -> None:
    endpoint = next(e for e in DIMENSIONS if e.table == "import_states")
    assert endpoint.path == "import-states"
    assert endpoint.primary_key == "import_key"
    assert endpoint.primary_key_type == "text"


def test_pages_follow_cursor_with_the_same_window(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_api(
        monkeypatch,
        [
            {"data": [_raw_row(0)], "next_cursor": "abc"},
            {"data": [_raw_row(1)], "next_cursor": None},
        ],
    )

    tables = _collect(RAW, T0, T0 + timedelta(hours=1))

    assert [t.num_rows for t in tables] == [1, 1]
    assert "cursor" not in calls[0]["params"]
    assert calls[1]["params"]["cursor"] == "abc"
    assert calls[0]["params"]["from"] == calls[1]["params"]["from"] == "2026-01-01T00:00:00.000000Z"
    assert calls[0]["params"]["to"] == calls[1]["params"]["to"] == "2026-01-01T01:00:00.000000Z"
    assert calls[0]["headers"] == {"Authorization": "Bearer token"}


def test_windows_cover_the_range_without_overlap(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_api(monkeypatch, [{"data": [], "next_cursor": None} for _ in range(3)])

    _collect(RAW, T0, T0 + timedelta(days=2, hours=12))

    assert [(c["params"]["from"][:13], c["params"]["to"][:13]) for c in calls] == [
        ("2026-01-01T00", "2026-01-02T00"),
        ("2026-01-02T00", "2026-01-03T00"),
        ("2026-01-03T00", "2026-01-03T12"),
    ]


def test_row_cap_stops_inside_a_window(monkeypatch: pytest.MonkeyPatch) -> None:
    capped = replace(RAW, max_rows_per_run=2)
    calls = _install_api(
        monkeypatch,
        [{"data": [_raw_row(m)], "next_cursor": str(m)} for m in range(5)],
    )

    tables = _collect(capped, T0, T0 + timedelta(days=9))

    assert len(calls) == 2
    assert sum(t.num_rows for t in tables) == 2


def test_empty_windows_do_not_stop_a_backfill(monkeypatch: pytest.MonkeyPatch) -> None:
    capped = replace(RAW, max_rows_per_run=1)
    _install_api(
        monkeypatch,
        [{"data": [], "next_cursor": None}, {"data": [_raw_row(0)], "next_cursor": None}],
    )

    tables = _collect(capped, T0, T0 + timedelta(days=2))

    assert [t.num_rows for t in tables] == [1]


def test_decimals_stay_exact_and_rollups_trim_to_the_column_scale() -> None:
    row = {
        "datetime": "2026-01-01T00:00:00.000000Z",
        "price_feed_id": 34462939,
        "price_avg": "975741540163.14317361111111111111111123456789",
        "price_high": "2",
        "price_low": "-1.5",
        "price_open": "0.000000000000000000000001",
        "price_close": "1234567890123456789012345678.1",
    }

    table = client.rows_to_arrow(ENDPOINTS["prices_1d"], [row])

    values = table.to_pylist()[0]
    assert values["price_avg"] == Decimal("975741540163.143173611111111111111111")
    assert values["price_open"] == Decimal("0.000000000000000000000001")
    assert values["price_close"] == Decimal("1234567890123456789012345678.1")
    assert values["datetime"] == T0


def test_nullable_columns_accept_nulls() -> None:
    row = {
        "datetime": "2026-01-01T00:00:00.000000Z",
        "product_id": 1,
        "block_number": 2,
        "vault_total_supply": None,
        "share_to_underlying_price": None,
        "staked_underlying": None,
        "underlying_total_supply": None,
        "underlying_capture_percentage": None,
    }

    table = client.rows_to_arrow(ENDPOINTS["vault_stats"], [row])

    assert table.column("vault_total_supply").null_count == 1
    assert not table.schema.field("product_id").nullable


def test_float_columns_accept_whole_numbers_above_2_pow_53() -> None:
    row = {name: None for name in ENDPOINTS["investor_timeline"].columns}
    row.update(
        datetime="2026-01-01T00:00:00.000000Z",
        investor_id=1,
        product_id=2,
        block_number=3,
        balance=9732405731723386,
        balance_diff=1.5,
    )

    table = client.rows_to_arrow(ENDPOINTS["investor_timeline"], [row])

    values = table.to_pylist()[0]
    assert values["balance"] == float(9732405731723386)
    assert values["balance_diff"] == 1.5
    assert values["usd_balance"] is None


def test_start_uses_the_head_lag_only_near_the_head() -> None:
    now = datetime(2026, 11, 1, tzinfo=timezone.utc)
    lag = RAW.lag

    assert source.start_datetime(RAW, None, now) == RAW.start
    backfilling = datetime(2023, 1, 1, tzinfo=timezone.utc)
    assert source.start_datetime(RAW, backfilling - lag, now) == backfilling
    assert source.start_datetime(RAW, now - lag - timedelta(minutes=5), now) == now - lag


def test_hourly_prices_start_inside_the_api_retention() -> None:
    hourly = ENDPOINTS["prices_1h"]
    now = datetime(2026, 11, 1, tzinfo=timezone.utc)

    assert source.start_datetime(hourly, None, now) == now - hourly.retention


@pytest.mark.parametrize("endpoint", TIME_SERIES, ids=lambda e: e.table)
def test_table_sql_is_partitioned_and_keys_are_not_null(endpoint: TimeSeries) -> None:
    sql = source.table_sql(endpoint)

    assert f"databarn_v1___{endpoint.table}" in sql
    assert "PARTITION BY toYYYYMM(datetime)" in sql
    assert set(endpoint.sort) <= set(endpoint.not_null)
