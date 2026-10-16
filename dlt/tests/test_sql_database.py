from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, TIMESTAMP

from lib.sql_database import time_bounded_select, time_window_bounds
from sources.resources.beefy_db.zap_common import ZAP_GENESIS, ZAP_WINDOW_DAYS


def _bindparams(clause):
    return clause.compile().params


def _bindparam_types(clause) -> dict[str, object]:
    return {name: param.type for name, param in clause._bindparams.items()}


def test_time_bounded_select_filters_by_time_and_ids() -> None:
    table = SimpleNamespace(fullname="prices")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 1, tzinfo=timezone.utc)

    clause = time_bounded_select(
        table,
        time_column="t",
        start_value=start,
        end_value=end,
        any_filters={"oracle_id": ("oracle_ids", [1, 2, 3])},
    )

    sql = str(clause)
    assert "FROM prices" in sql
    assert "oracle_id = ANY(:oracle_ids)" in sql
    assert "t > :start_value" in sql
    assert "t <= :end_value" in sql
    params = _bindparams(clause)
    assert params["start_value"] == start
    assert params["end_value"] == end
    assert params["oracle_ids"] == [1, 2, 3]
    types = _bindparam_types(clause)
    assert isinstance(types["start_value"], TIMESTAMP)
    assert types["start_value"].timezone is True
    assert isinstance(types["end_value"], TIMESTAMP)
    assert types["end_value"].timezone is True
    assert isinstance(types["oracle_ids"], ARRAY)
    assert isinstance(types["oracle_ids"].item_type, BIGINT)


def test_time_bounded_select_coerces_naive_datetimes_to_utc() -> None:
    table = SimpleNamespace(fullname="harvests")
    start = datetime(2026, 10, 14, 16, 52, 7)
    end = datetime(2027, 2, 11, 16, 52, 7)

    clause = time_bounded_select(
        table,
        time_column="txn_timestamp",
        start_value=start,
        end_value=end,
    )
    params = _bindparams(clause)
    assert params["start_value"] == start.replace(tzinfo=timezone.utc)
    assert params["end_value"] == end.replace(tzinfo=timezone.utc)


def test_time_bounded_select_rejects_bad_identifiers() -> None:
    table = SimpleNamespace(fullname="prices")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 5, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        time_bounded_select(
            table,
            time_column="t; drop table prices",
            start_value=start,
            end_value=end,
        )


def test_time_window_bounds_uses_incremental_cursor() -> None:
    start = datetime(2024, 6, 1, tzinfo=timezone.utc)
    incremental = SimpleNamespace(start_value=start)
    window_start, window_end = time_window_bounds(
        incremental, default_start=datetime(2021, 7, 31, tzinfo=timezone.utc), window_days=120
    )
    assert window_start == start
    assert (window_end - window_start).days == 120


def test_time_window_bounds_falls_back_to_default() -> None:
    default = datetime(2021, 7, 31, tzinfo=timezone.utc)
    window_start, window_end = time_window_bounds(
        SimpleNamespace(start_value=None), default_start=default, window_days=120
    )
    assert window_start == default
    assert (window_end - window_start).days == 120


def test_time_bounded_select_hex_encodes_bytea_columns() -> None:
    table = SimpleNamespace(
        fullname="zap_parent_transactions",
        c=[
            SimpleNamespace(name="txn_hash"),
            SimpleNamespace(name="value"),
            SimpleNamespace(name="calldata"),
        ],
    )
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 31, tzinfo=timezone.utc)

    clause = time_bounded_select(
        table,
        time_column="txn_timestamp",
        start_value=start,
        end_value=end,
        hex_columns={"txn_hash", "calldata"},
    )

    sql = str(clause)
    assert "FROM zap_parent_transactions" in sql
    assert "'0x' || encode(txn_hash, 'hex') AS txn_hash" in sql
    assert "'0x' || encode(calldata, 'hex') AS calldata" in sql
    assert "value" in sql
    assert "SELECT *" not in sql
    assert "txn_timestamp > :start_value" in sql
    assert "txn_timestamp <= :end_value" in sql


def test_time_bounded_select_serializes_postgres_arrays() -> None:
    table = SimpleNamespace(
        fullname="zap_records",
        c=[
            SimpleNamespace(name="swap_source"),
            SimpleNamespace(name="updated_at"),
        ],
    )
    start = datetime(2026, 8, 13, tzinfo=timezone.utc)
    end = datetime(2026, 9, 12, tzinfo=timezone.utc)

    clause = time_bounded_select(
        table,
        time_column="updated_at",
        start_value=start,
        end_value=end,
        json_array_columns={"swap_source"},
    )

    sql = str(clause)
    assert "CAST(array_to_json(swap_source) AS text) AS swap_source" in sql
    assert "updated_at > :start_value" in sql
    assert "updated_at <= :end_value" in sql


def test_zap_genesis_is_before_first_created_at() -> None:
    first_row = datetime(2026, 8, 13, 5, 48, 24, tzinfo=timezone.utc)
    assert ZAP_GENESIS < first_row
    assert ZAP_WINDOW_DAYS == 30
