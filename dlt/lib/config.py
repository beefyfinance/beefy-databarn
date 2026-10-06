"""
Read-only config helpers. DLT is configured via env vars; map your .env in infra/dlt/set_dlt_env.sh.
See infra/dlt/set_dlt_env.sh for .env → DLT env name mapping.
"""
import os
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

BATCH_SIZE = 1_000_000
# Full-replace zap tables include hex-encoded calldata. yield_per=BATCH_SIZE
# asks Postgres to hold ~1M wide rows in PortalHoldContext and OOMs.
ZAP_BATCH_SIZE = 10_000

# Pipeline iteration timeout (seconds)
PIPELINE_ITERATION_TIMEOUT = int(os.environ.get("DLT_PIPELINE_ITERATION_TIMEOUT", "3600"))

# clickhouse_connect HTTP read timeout (seconds). OPTIMIZE FINAL can exceed the 300s default.
CLICKHOUSE_SEND_RECEIVE_TIMEOUT = int(os.environ.get("DLT_CLICKHOUSE_SEND_RECEIVE_TIMEOUT", "3600"))


# Heroku/AWS Postgres needs TLS. Newer libpq also tries GSS first; disable it.
_DEFAULT_SSLMODE = "require"
_DEFAULT_GSSENCMODE = "disable"
_DEFAULT_CONNECT_TIMEOUT_S = "10"


def _with_postgres_connect_params(url: str) -> str:
    """Fill in SSL/GSS/timeout params without overriding values already in the DSN."""
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params.setdefault("sslmode", os.environ.get("BEEFY_DB_SSLMODE", _DEFAULT_SSLMODE))
    params.setdefault("gssencmode", _DEFAULT_GSSENCMODE)
    params.setdefault("connect_timeout", _DEFAULT_CONNECT_TIMEOUT_S)
    return urlunparse(parsed._replace(query=urlencode(params)))


def get_beefy_db_url() -> str:
    """Beefy DB connection string (set by infra/dlt/set_dlt_env.sh from BEEFY_DB_* → SOURCES__BEEFY_DB__CREDENTIALS)."""
    url = os.environ.get("SOURCES__BEEFY_DB__CREDENTIALS")
    if not url:
        raise ValueError(
            "SOURCES__BEEFY_DB__CREDENTIALS not set. Source infra/dlt/set_dlt_env.sh or set BEEFY_DB_* / SOURCES__BEEFY_DB__CREDENTIALS."
        )
    return _with_postgres_connect_params(url)


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
