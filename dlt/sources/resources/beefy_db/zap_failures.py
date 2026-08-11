from typing import Any

from dlt.sources.sql_database import sql_table

from lib.config import BATCH_SIZE, get_beefy_db_url
from lib.sql_database import hex_encode_bytea_columns


async def get_beefy_db_zap_failures_resource() -> Any:
    columns = {
        "chain_id": {
            "data_type": "bigint",
            "primary_key": True,
            "nullable": False,
        },
        "block_number": {
            "data_type": "bigint",
            "nullable": False,
        },
        "txn_hash": {
            "data_type": "text",
            "primary_key": True,
            "nullable": False,
        },
        "from_address": {
            "data_type": "text",
            "nullable": False,
        },
        "input": {
            "data_type": "text",
            "nullable": False,
        },
        "created_at": {
            "data_type": "timestamp",
            "nullable": False,
        },
    }

    pk = [name for name, c in columns.items() if c.get("primary_key")]
    resource = sql_table(
        credentials=get_beefy_db_url(),
        table="zap_failures",
        backend="pyarrow",
        chunk_size=BATCH_SIZE,
        backend_kwargs={"tz": "UTC"},
        reflection_level="full_with_precision",
        # txn_hash / from_address are bpchar (already text); only input is bytea
        query_adapter_callback=hex_encode_bytea_columns({"input"}),
        primary_key=pk,
        write_disposition="replace",
    )
    resource.apply_hints(columns=[{"name": name, **c} for name, c in columns.items()])
    return resource
