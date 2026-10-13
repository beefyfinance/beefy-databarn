from typing import Any

import dlt
from dlt.destinations.adapters import clickhouse_adapter
from dlt.sources.sql_database import sql_table

from lib.config import ZAP_BATCH_SIZE, get_beefy_db_url
from sources.resources.beefy_db.zap_common import zap_windowed_query_adapter

RESOURCE_NAME = "zap_records"
HEX_COLUMNS = {"txn_hash", "caller_address", "recipient_address", "cctp_hash"}


async def get_beefy_db_zap_records_resource() -> Any:
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
        "event_idx": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
        },
        "txn_hash": {
            "data_type": "text",
            "nullable": False,
        },
        "caller_address": {
            "data_type": "text",
            "nullable": False,
        },
        "recipient_address": {
            "data_type": "text",
            "nullable": False,
        },
        "source_chain_id": {
            "data_type": "bigint",
        },
        "target_chain_id": {
            "data_type": "bigint",
        },
        "vault_id": {
            "data_type": "text",
        },
        "action": {
            "data_type": "text",
        },
        "total_usd": {
            "data_type": "decimal",
            "scale": 18,
            "precision": 30,
        },
        "cctp_hash": {
            "data_type": "text",
        },
        "swap_source": {
            "data_type": "text",
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
        table=RESOURCE_NAME,
        backend="pyarrow",
        chunk_size=ZAP_BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        query_adapter_callback=zap_windowed_query_adapter(
            RESOURCE_NAME,
            "updated_at",
            hex_columns=HEX_COLUMNS,
            json_array_columns={"swap_source"},
        ),
        primary_key=pk,
        write_disposition="append",
        incremental=incremental,
    )
    resource.apply_hints(columns=[{"name": name, **c} for name, c in columns.items()])
    return clickhouse_adapter(resource, table_engine_type="replacing_merge_tree")
