from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Set

from lib.sql_database import time_bounded_select, time_window_bounds

logger = logging.getLogger(__name__)

# First zap rows landed 2026-08-13 05:48 UTC (created_at / updated_at).
# Window start is exclusive, so midnight UTC that day still includes them.
ZAP_GENESIS = datetime(2026, 8, 13, 0, 0, 0, tzinfo=timezone.utc)
ZAP_WINDOW_DAYS = 30


def zap_windowed_query_adapter(
    resource_name: str,
    time_column: str,
    *,
    hex_columns: Optional[Set[str]] = None,
    json_array_columns: Optional[Set[str]] = None,
) -> Callable[..., Any]:
    def query_adapter_callback(query, table, incremental=None, engine=None):
        start_value, end_value = time_window_bounds(
            incremental,
            default_start=ZAP_GENESIS,
            window_days=ZAP_WINDOW_DAYS,
        )
        logger.info("%s_query_adapter_callback: %s %s", resource_name, start_value, end_value)
        return time_bounded_select(
            table,
            time_column=time_column,
            start_value=start_value,
            end_value=end_value,
            hex_columns=hex_columns,
            json_array_columns=json_array_columns,
        )

    return query_adapter_callback
