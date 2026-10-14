from __future__ import annotations

import logging
import time
from functools import lru_cache
from typing import Any

import psycopg2
import sqlalchemy as sa

from lib.config import (
    BEEFY_TIMESCALEDB_POOL_SIZE,
    PIPELINE_ITERATION_TIMEOUT,
    get_beefy_db_url,
    get_beefy_timescaledb_url,
)

logger = logging.getLogger(__name__)

# One try plus a few retries. Backoff doubles: 1s, 2s, 4s.
_MAX_ATTEMPTS = 4
_RETRY_BACKOFF_S = 1.0


def connect_postgres(url: str, label: str) -> Any:
    """Connect to Postgres, retrying transient libpq/SSL handshake failures.

    libpq 17+ (CVE-2024-10977) hides the postmaster error during SSL setup, so
    "too many connections" / a brief restart surfaces as "error response during
    SSL exchange". A short retry usually succeeds once a slot frees up.
    """
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(url)
        except psycopg2.OperationalError as err:
            if attempt >= _MAX_ATTEMPTS:
                raise
            delay = _RETRY_BACKOFF_S * (2 ** (attempt - 1))
            logger.warning(
                "%s connect failed (%s); retry %s/%s in %.0fs",
                label,
                err,
                attempt,
                _MAX_ATTEMPTS - 1,
                delay,
            )
            time.sleep(delay)


def connect_beefy_db() -> Any:
    """Connect to Heroku beefy-db. Fallback; ingestions use TimescaleDB."""
    return connect_postgres(get_beefy_db_url(), "Beefy DB")


def connect_beefy_timescaledb() -> Any:
    """Connect to the Tiger Cloud Timescale instance of migrated beefy-db tables."""
    return connect_postgres(get_beefy_timescaledb_url(), "Beefy Timescale DB")


@lru_cache(maxsize=1)
def get_beefy_timescaledb_engine() -> Any:
    """Shared Timescale engine capped at the prod role connection budget.

    ``sql_table(credentials=url)`` would otherwise build a QueuePool per table.
    Parallel extract then opens one connection per resource and the Timescale
    role hits ``too many connections``. Checkout waits if extract still fans out.
    """
    url = get_beefy_timescaledb_url()

    def creator() -> Any:
        return connect_postgres(url, "Beefy Timescale DB")

    return sa.create_engine(
        url,
        creator=creator,
        pool_size=BEEFY_TIMESCALEDB_POOL_SIZE,
        max_overflow=0,
        pool_timeout=PIPELINE_ITERATION_TIMEOUT,
        pool_pre_ping=True,
    )


def fetch_timescaledb_ints(sql: str) -> list[int]:
    """Run a single-column integer query on the shared Timescale engine."""
    with get_beefy_timescaledb_engine().connect() as conn:
        return [int(row[0]) for row in conn.execute(sa.text(sql))]
