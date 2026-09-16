import logging
from typing import Any
from datetime import datetime, timedelta, timezone
import dlt
import psycopg2
import sqlalchemy as sa
from dlt.sources.sql_database import sql_table
from lib.config import BATCH_SIZE, get_beefy_db_url
from lib.clickhouse import get_clickhouse_client

logger = logging.getLogger(__name__)

DATE_RANGE_SIZE_IN_DAYS = 120

SOURCE_NAME = "beefy_db"
RESOURCE_NAME = "apys"
FULL_TABLE_NAME = f"{SOURCE_NAME}___{RESOURCE_NAME}"

# custom sql to use ReplacingMergeTree and compression codecs
TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME}
    (
        `vault_id` Int64 CODEC(ZSTD(3)),
        `t`         DateTime64(6, 'UTC') CODEC(ZSTD(3)),
        `val`       Nullable(Float64) CODEC(ZSTD(3))
    )
    ENGINE = ReplacingMergeTree
    PRIMARY KEY (vault_id, t)
    ORDER BY (vault_id, t)
    SETTINGS index_granularity = 8192;
"""

VAULT_IDS_SQL = "SELECT DISTINCT id FROM vault_ids"


async def _init_resource() -> list[int]:
    client = await get_clickhouse_client()
    await client.query(TABLE_SQL)

    conn = psycopg2.connect(get_beefy_db_url())
    try:
        with conn.cursor() as cur:
            cur.execute(VAULT_IDS_SQL)
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
    

async def get_beefy_db_apys_resource() -> Any:
    vault_ids = await _init_resource()

    # # APYs table
    def apys_query_adapter_callback(query, table, incremental=None, engine=None):
        start_value = incremental.start_value
        if start_value is None:
            start_value = datetime(2021, 7, 31, 0, 0, 0, tzinfo=timezone.utc) #  2021-07-31 19:30:00+00

        end_value = start_value + timedelta(days=DATE_RANGE_SIZE_IN_DAYS)

        logger.info(f"apys_query_adapter_callback: {start_value} {end_value}")

        return sa.text(f"""
            SELECT * 
            FROM {table.fullname}
            WHERE vault_id = ANY(:vault_ids)
            AND t > :start_value
            AND t <= :end_value
        """).bindparams(**{
            "start_value": start_value,
            "end_value": end_value,
            "vault_ids": vault_ids,
        })

    incremental = dlt.sources.incremental(
        "t",
        initial_value=None,
        primary_key=["vault_id", "t"],
        last_value_func=max,
        row_order="asc",
    )
    # one timestamp is shared across many vaults
    incremental.duplicate_cursor_warning_threshold = 10_000

    apys = sql_table(
        credentials=get_beefy_db_url(),
        table=RESOURCE_NAME,
        backend="pyarrow",
        chunk_size=BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        query_adapter_callback=apys_query_adapter_callback,
        primary_key=["vault_id", "t"],
        write_disposition="append",
        incremental=incremental,
    )
    apys.apply_hints(
        columns=[
            # force keys to be non-nullable
            {"name": "vault_id", "nullable": False },
            {"name": "t", "nullable": False },

            # keep val as float64 because some values are too large for Decimal(76, 20)
            {"name": "val", "data_type": "double"},
        ]
    )

    return apys
