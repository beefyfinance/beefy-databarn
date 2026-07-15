from typing import Any
from lib.config import BATCH_SIZE, get_beefy_db_url
from lib.sql_database import try_sql_table

def get_beefy_db_other_tables_resources() -> list[Any]:
    
    db_url = get_beefy_db_url()

    tables = {
        "address_metadata": [
            {"name": "chain_id", "primary_key": True },
            {"name": "address", "primary_key": True },
        ],
        "bifi_buyback": [
            {"name": "id", "primary_key": True },
            {"name": "bifi_amount", "data_type": "decimal" },
            {"name": "bifi_price", "data_type": "decimal" },
            {"name": "buyback_total", "data_type": "decimal" },
        ],
        "chains": [
            {"name": "chain_id", "primary_key": True },
        ],
        "price_oracles": [{"name": "id", "primary_key": True }],
        "vault_ids": [{"name": "id", "primary_key": True }],
        "vault_strategies": [
            {"name": "id", "primary_key": True },
        ],
        "feebatch_harvests": [
            {"name": "chain_id", "primary_key": True },
            {"name": "block_number", "primary_key": True },
        ]
    }

    resources = []
    for table_name, columns in tables.items():
        primary_key_columns = [column["name"] for column in columns if "primary_key" in column and column["primary_key"]]
        resource = try_sql_table(
            credentials=db_url,
            table=table_name,
            backend="sqlalchemy",
            chunk_size=BATCH_SIZE,
            backend_kwargs={"tz": "UTC"},
            reflection_level="full_with_precision",
            primary_key=primary_key_columns,
            write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        )
        if resource is None:
            continue
        resource.apply_hints(columns=columns)
        resources.append(resource)

    return resources
