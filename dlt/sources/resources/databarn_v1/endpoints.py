"""Tables served by the export API of databarn v1 (beefy-bi), and how to page through them.

investor-counts is deliberately left out.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

import pyarrow as pa

# Exact API values have 24 decimals. Only the 1h and 1d rollups have more, and lose them.
DECIMAL = pa.decimal256(76, 24)
TIMESTAMP = pa.timestamp("us", tz="UTC")

_FIRST_ROW = datetime(2020, 10, 1, tzinfo=timezone.utc)
_DEFAULT_MAX_ROWS_PER_RUN = 1_000_000


@dataclass(frozen=True)
class Dimension:
    table: str
    path: str
    primary_key: str
    json_column: Optional[str] = None
    primary_key_type: str = "bigint"


@dataclass(frozen=True)
class TimeSeries:
    table: str
    path: str
    columns: Dict[str, pa.DataType]
    not_null: Tuple[str, ...]
    # Unique per row, as read from the API cursor. Sorted to compress best.
    sort: Tuple[str, ...]
    # Width of each from/to request: a wide one on prices/1h is a gateway 504.
    window: timedelta
    # Re-read to pick up open rollup buckets and rows imported late.
    lag: timedelta
    start: datetime = _FIRST_ROW
    retention: Optional[timedelta] = None
    # A backfill resumes on the next run. A run must load inside the 30 minute scheduler timeout.
    max_rows_per_run: int = _DEFAULT_MAX_ROWS_PER_RUN


_ROLLUP_COLUMNS: Dict[str, pa.DataType] = {
    "datetime": TIMESTAMP,
    "price_feed_id": pa.int64(),
    "price_avg": DECIMAL,
    "price_high": DECIMAL,
    "price_low": DECIMAL,
    "price_open": DECIMAL,
    "price_close": DECIMAL,
}
_ROLLUP_NOT_NULL = tuple(_ROLLUP_COLUMNS)

DIMENSIONS = (
    Dimension("products", "products", "product_id", json_column="product_data"),
    Dimension("price_feeds", "price-feeds", "price_feed_id", json_column="price_feed_data"),
    Dimension("investors", "investors", "investor_id"),
    Dimension(
        "import_states",
        "import-states",
        "import_key",
        json_column="import_data",
        primary_key_type="text",
    ),
)

TIME_SERIES = (
    TimeSeries(
        table="prices_raw",
        path="prices/raw",
        columns={
            "datetime": TIMESTAMP,
            "price_feed_id": pa.int64(),
            "block_number": pa.int64(),
            "price": DECIMAL,
        },
        not_null=("datetime", "price_feed_id", "block_number", "price"),
        sort=("price_feed_id", "datetime", "block_number"),
        window=timedelta(days=1),
        lag=timedelta(hours=6),
        # About 650M rows: 300k a day since 2020.
        max_rows_per_run=5_000_000,
    ),
    TimeSeries(
        table="prices_1h",
        path="prices/1h",
        columns=_ROLLUP_COLUMNS,
        not_null=_ROLLUP_NOT_NULL,
        sort=("price_feed_id", "datetime"),
        window=timedelta(days=1),
        lag=timedelta(hours=6),
        # The API keeps 6 months; stay a few days inside so a backfill never starts on the edge.
        retention=timedelta(days=175),
    ),
    TimeSeries(
        table="prices_1d",
        path="prices/1d",
        columns=_ROLLUP_COLUMNS,
        not_null=_ROLLUP_NOT_NULL,
        sort=("price_feed_id", "datetime"),
        window=timedelta(days=7),
        lag=timedelta(days=2),
    ),
    TimeSeries(
        table="vault_stats",
        path="vault-stats",
        columns={
            "datetime": TIMESTAMP,
            "product_id": pa.int64(),
            "block_number": pa.int64(),
            "vault_total_supply": DECIMAL,
            "share_to_underlying_price": DECIMAL,
            "staked_underlying": DECIMAL,
            "underlying_total_supply": DECIMAL,
            "underlying_capture_percentage": pa.float64(),
        },
        not_null=("datetime", "product_id", "block_number"),
        sort=("product_id", "datetime", "block_number"),
        window=timedelta(days=30),
        lag=timedelta(hours=6),
    ),
    TimeSeries(
        table="investor_timeline",
        path="investor-timeline",
        columns={
            "datetime": TIMESTAMP,
            "investor_id": pa.int64(),
            "product_id": pa.int64(),
            "block_number": pa.int64(),
            "transaction_hash": pa.string(),
            "balance": pa.float64(),
            "balance_diff": pa.float64(),
            "share_to_underlying_price": pa.float64(),
            "underlying_balance": pa.float64(),
            "underlying_diff": pa.float64(),
            "underlying_to_usd_price": pa.float64(),
            "usd_balance": pa.float64(),
            "usd_diff": pa.float64(),
            "pending_rewards": pa.float64(),
            "pending_rewards_diff": pa.float64(),
            "pending_rewards_to_usd_price": pa.float64(),
            "pending_rewards_usd_balance": pa.float64(),
            "pending_rewards_usd_diff": pa.float64(),
        },
        not_null=("datetime", "investor_id", "product_id", "block_number"),
        sort=("product_id", "investor_id", "datetime", "block_number"),
        window=timedelta(days=7),
        lag=timedelta(hours=6),
    ),
)
