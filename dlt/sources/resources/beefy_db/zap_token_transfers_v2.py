from typing import Any

import dlt
from dlt.destinations.adapters import clickhouse_adapter
from dlt.sources.sql_database import sql_table

from lib.config import ZAP_BATCH_SIZE, get_beefy_db_url
from sources.resources.beefy_db.zap_common import zap_windowed_query_adapter

RESOURCE_NAME = "zap_token_transfers_v2"


async def get_beefy_db_zap_token_transfers_v2_resource() -> Any:
    columns = {
        "chain_id": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
        },
        "block_number": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
        },
        "txn_idx": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
        },
        "parent_event_idx": {
            "data_type": "bigint",
            "nullable": False,
        },
        "event_idx": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
        },
        "zap_type": {
            "data_type": "text",
            "nullable": False,
        },
        "token_id": {
            "data_type": "bigint",
        },
        "token_amount": {
            "data_type": "decimal",
            "scale": 18,
            "precision": 36,
            "nullable": False,
        },
        "usd_value": {
            "data_type": "decimal",
            "scale": 18,
            "precision": 30,
        },
        "token_address": {
            "data_type": "text",
            "nullable": False,
        },
        "created_at": {
            "data_type": "timestamp",
            "nullable": False,
        },
        "updated_at": {
            "data_type": "timestamp",
            "nullable": False,
        },
    }

    pk = [name for name, c in columns.items() if c.get("primary_key")]
    incremental = dlt.sources.incremental(
        "updated_at",
        initial_value=None,
        primary_key=pk,
        last_value_func=max,
        row_order="asc",
    )
    incremental.duplicate_cursor_warning_threshold = 10_000

    resource = sql_table(
        credentials=get_beefy_db_url(),
        # Source Postgres table is still zap_token_transfers; destination
        # resource is renamed to avoid colliding with the legacy CH table.
        table="zap_token_transfers",
        backend="pyarrow",
        chunk_size=ZAP_BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        query_adapter_callback=zap_windowed_query_adapter(
            RESOURCE_NAME, "updated_at", hex_columns={"token_address"}
        ),
        primary_key=pk,
        write_disposition="append",
        incremental=incremental,
    )
    resource.apply_hints(columns=[{"name": name, **c} for name, c in columns.items()])
    return clickhouse_adapter(
        resource.with_name(RESOURCE_NAME),
        table_engine_type="replacing_merge_tree",
    )
