"""
Read-only config helpers. DLT is configured via env vars; map your .env in infra/dlt/set_dlt_env.sh.
See infra/dlt/set_dlt_env.sh for .env → DLT env name mapping.
"""
import os

BATCH_SIZE = 1_000_000

# Pipeline iteration timeout (seconds)
PIPELINE_ITERATION_TIMEOUT = int(os.environ.get("DLT_PIPELINE_ITERATION_TIMEOUT", "3600"))


def get_beefy_db_url() -> str:
    """Beefy DB connection string (set by infra/dlt/set_dlt_env.sh from BEEFY_DB_* → SOURCES__BEEFY_DB__CREDENTIALS)."""
    url = os.environ.get("SOURCES__BEEFY_DB__CREDENTIALS")
    if not url:
        raise ValueError(
            "SOURCES__BEEFY_DB__CREDENTIALS not set. Source infra/dlt/set_dlt_env.sh or set BEEFY_DB_* / SOURCES__BEEFY_DB__CREDENTIALS."
        )
    return url


def get_clickhouse_credentials() -> dict:
    """ClickHouse credentials dict (from DESTINATION__CLICKHOUSE__CREDENTIALS__* set by infra/dlt/set_dlt_env.sh)."""
    host = os.environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST")
    user = os.environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME")
    password = os.environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD")
    database = os.environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE")
    if not all([host, user, password, database]):
        raise ValueError(
            "ClickHouse env not set. Source infra/dlt/set_dlt_env.sh or set DLT_CLICKHOUSE_* / DESTINATION__CLICKHOUSE__CREDENTIALS__*."
        )
    port = int(os.environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__PORT", "9000"))
    http_port = int(os.environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT", "8123"))
    secure = int(os.environ.get("DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE", "0"))
    return {
        "host": host,
        "port": port,
        "http_port": http_port,
        "username": user,
        "user": user,
        "password": password,
        "database": database,
        "secure": secure,
    }
