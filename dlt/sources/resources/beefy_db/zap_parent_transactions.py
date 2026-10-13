from typing import Any

import dlt
from dlt.destinations.adapters import clickhouse_adapter
from dlt.sources.sql_database import sql_table

from lib.config import ZAP_BATCH_SIZE, get_beefy_timescaledb_url
from sources.resources.beefy_db.zap_common import zap_windowed_query_adapter

RESOURCE_NAME = "zap_parent_transactions"
HEX_COLUMNS = {"txn_hash", "from_address", "to_address", "calldata"}


async def get_beefy_db_zap_parent_transactions_resource() -> Any:
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
        "txn_timestamp": {
            "data_type": "timestamp",
            "nullable": False,
        },
        "txn_hash": {
            "data_type": "text",
            "nullable": False,
        },
        "value": {
            "data_type": "decimal",
            "scale": 18,
            "precision": 36,
            "nullable": False,
        },
        "effective_gas_price": {
            "data_type": "bigint",
            "nullable": False,
        },
        "gas_limit": {
            "data_type": "bigint",
            "nullable": False,
        },
        "gas_used": {
            "data_type": "bigint",
            "nullable": False,
        },
        "native_price": {
            "data_type": "decimal",
            "scale": 18,
            "precision": 30,
        },
        "from_address": {
            "data_type": "text",
            "nullable": False,
        },
        "to_address": {
            "data_type": "text",
            "nullable": False,
        },
        "calldata": {
            "data_type": "text",
            "nullable": False,
        },
        "success": {
            "data_type": "bool",
            "nullable": False,
        },
        "created_at": {
            "data_type": "timestamp",
            "nullable": False,
        },
    }

    pk = [name for name, c in columns.items() if c.get("primary_key")]
    incremental = dlt.sources.incremental(
        "created_at",
        initial_value=None,
        primary_key=pk,
        last_value_func=max,
        row_order="asc",
    )
    incremental.duplicate_cursor_warning_threshold = 10_000

    resource = sql_table(
        credentials=get_beefy_timescaledb_url(),
        table=RESOURCE_NAME,
        backend="pyarrow",
        chunk_size=ZAP_BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        query_adapter_callback=zap_windowed_query_adapter(
            RESOURCE_NAME, "created_at", hex_columns=HEX_COLUMNS
        ),
        primary_key=pk,
        write_disposition="append",
        incremental=incremental,
    )
    resource.apply_hints(columns=[{"name": name, **c} for name, c in columns.items()])
    return clickhouse_adapter(resource, table_engine_type="replacing_merge_tree")
