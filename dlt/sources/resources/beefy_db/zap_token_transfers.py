from typing import Any
import dlt
from dlt.destinations.adapters import clickhouse_adapter
from dlt.sources.sql_database import sql_table
from lib.config import BATCH_SIZE, get_beefy_db_url

async def get_beefy_db_zap_token_transfers_resource() -> Any:

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
        "parent_event_idx": {
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
        "zap_type": {
            "data_type": "text",
            "nullable": False,
            "codec": "LZ4",
        },
        "token_id": {
            "data_type": "bigint",
            "codec": "LZ4",
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
            "codec": "LZ4",
        },
    }

    pk = [name for name, c in columns.items() if c.get("primary_key")]
    zap_token_transfers = sql_table(
        credentials=get_beefy_db_url(),
        table="zap_token_transfers",
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
    zap_token_transfers.apply_hints(
        columns=[{"name": name, **{k: v for k, v in c.items() if k != "codec"}} for name, c in columns.items()]
    )

    zap_token_transfers = clickhouse_adapter(
        zap_token_transfers,
        table_engine_type="replacing_merge_tree",
        sort=pk,
        settings={"allow_nullable_key": False},
        codecs={name: c["codec"] for name, c in columns.items() if "codec" in c}
    )

    return zap_token_transfers
