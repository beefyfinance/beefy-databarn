from __future__ import annotations

import lib.dlt_clickhouse_truncate_if_exists_patch as patch
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient


def test_truncate_uses_if_exists() -> None:
    sql = patch._truncate_table_sql(
        None,  # type: ignore[arg-type]
        "`dlt`.`beefy_api_staging___treasury_mm`",
    )
    assert sql == "TRUNCATE TABLE IF EXISTS `dlt`.`beefy_api_staging___treasury_mm`"
    assert ClickHouseSqlClient._truncate_table_sql is patch._truncate_table_sql
