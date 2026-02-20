from typing import Any
import dlt
from dlt.destinations.adapters import clickhouse_adapter
from dlt.sources.sql_database import sql_table
from lib.config import BATCH_SIZE, get_beefy_db_url

async def get_beefy_db_zap_events_resource() -> Any:

    # data_type docs: https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas#supported-arrow-data-types
    columns = {
        "chain_id": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
            "codec": "LZ4",
        },
        "block_number": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
            "codec": "LZ4",
        },
        "txn_idx": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
            "codec": "LZ4",
        },
        "event_idx": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
            "codec": "LZ4",
        },
        "txn_timestamp": {
            "data_type": "timestamp",
            "nullable": False,
            "codec": "ZSTD(3)",
        },
        "txn_hash": {
            "data_type": "text",
            "nullable": False,
            "codec": "LZ4",
        },
        "caller_address": {
            "data_type": "text",
            "nullable": False,
            "codec": "LZ4",
        },
        "recipient_address": {
            "data_type": "text",
            "nullable": False,
            "codec": "LZ4",
        },
        "target_chain_id": {
            "data_type": "bigint",
            "nullable": False,
            "codec": "LZ4",
        },
        "vault_id": {
            "data_type": "text",
            "codec": "LZ4",
        },
        "action": {
            "data_type": "text",
            "codec": "LZ4",
        },
        "total_usd": {
            "data_type": "decimal",
            "scale": 18,
            "precision": 30,
        },
        "updated_at": {
            "data_type": "timestamp",
            "nullable": False,
            "codec": "ZSTD(3)",
        },
        "swap_source": {
            "data_type": "text",
            "codec": "LZ4",
        },
    }

    pk = [name for name, c in columns.items() if c.get("primary_key")]
    zap_events = sql_table(
        credentials=get_beefy_db_url(),
        table="zap_events",
        backend="pyarrow",
        chunk_size=BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        primary_key=pk,
        write_disposition="append", 
        incremental=dlt.sources.incremental(
            "updated_at", 
            initial_value=None,
            primary_key=pk,
            last_value_func=max,
            row_order="asc" 
        ),
    )
    zap_events.apply_hints(
        columns=[{"name": name, **{k: v for k, v in c.items() if k != "codec"}} for name, c in columns.items()]
    )

    zap_events = clickhouse_adapter(
        zap_events, 
        table_engine_type="replacing_merge_tree",
        sort=pk,
        settings={"allow_nullable_key": False},
        codecs={ name: c["codec"] for name, c in columns.items() if "codec" in c}
    )

    return zap_events


