from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lib.sql_database import time_bounded_select, time_window_bounds


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
    assert clause.compile().params["start_value"] == start
    assert clause.compile().params["end_value"] == end
    assert clause.compile().params["oracle_ids"] == [1, 2, 3]


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
