"""TRUNCATE TABLE IF EXISTS on ClickHouse staging cleanup.

dlt's end-of-load staging truncate (`truncate_staging_dataset`) issues
TRUNCATE for every schema table that uses a staging dataset. Leftover
tables from retired resources (for example `treasury_mm`) stay in the
stored schema but were never created in ClickHouse, so TRUNCATE without
IF EXISTS logs a Code 60 warning and a full stack trace. DROP TABLE in
this client already uses IF EXISTS.
"""

from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient


def _truncate_table_sql(self: ClickHouseSqlClient, qualified_table_name: str) -> str:
    return f"TRUNCATE TABLE IF EXISTS {qualified_table_name}"


ClickHouseSqlClient._truncate_table_sql = _truncate_table_sql  # type: ignore[method-assign]
