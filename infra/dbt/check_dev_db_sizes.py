"""Daily size check for developer sandbox databases (invoked by the scheduler)."""
import logging
import os

import clickhouse_connect

import alerts

logger = logging.getLogger(__name__)

GIB = 1024**3
SIZES_QUERY = (
    "SELECT database, sum(bytes_on_disk) FROM system.parts "
    "WHERE active AND startsWith(database, 'dev_') GROUP BY database ORDER BY 2 DESC"
)


def find_oversized(rows, limit_gb: float) -> list[tuple[str, float]]:
    return [(db, size / GIB) for db, size in rows if size > limit_gb * GIB]


def check_dev_db_sizes():
    """Alert on Discord when a dev_* database exceeds DEV_DB_MAX_SIZE_GB."""
    try:
        limit_gb = float(os.environ.get("DEV_DB_MAX_SIZE_GB", "20"))
        client = clickhouse_connect.get_client(
            host=os.environ.get("DBT_CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.environ.get("DBT_CLICKHOUSE_PORT", "8123")),
            username=os.environ.get("DBT_CLICKHOUSE_USER", "dbt"),
            password=os.environ["DBT_CLICKHOUSE_PASSWORD"],
        )
        rows = client.query(SIZES_QUERY).result_rows
        oversized = find_oversized(rows, limit_gb)
        logger.info("Dev DB sizes: %d database(s) checked, %d over %g GB", len(rows), len(oversized), limit_gb)
        if oversized:
            alerts.alert_dev_db_sizes(oversized, limit_gb)
    except Exception as e:
        logger.error(f"Error checking dev database sizes: {e}", exc_info=True)
        alerts.alert_dev_db_size_exception(e)
