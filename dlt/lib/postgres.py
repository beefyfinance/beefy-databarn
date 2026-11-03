from __future__ import annotations

import asyncio
import logging
import time
from functools import lru_cache
from typing import Any, Awaitable, Callable, TypeVar

import psycopg2
import sqlalchemy as sa

from lib.config import (
    BEEFY_TIMESCALEDB_POOL_SIZE,
    PIPELINE_ITERATION_TIMEOUT,
    get_beefy_db_url,
    get_beefy_timescaledb_url,
)
import lib.dlt_timescaledb_no_cursor_patch  # noqa: F401

logger = logging.getLogger(__name__)

# One try plus a few retries. Backoff doubles: 1s, 2s, 4s.
_MAX_ATTEMPTS = 4
_RETRY_BACKOFF_S = 1.0

T = TypeVar("T")


def _retry_delay(label: str, what: str, err: BaseException, attempt: int) -> float:
    delay = _RETRY_BACKOFF_S * (2 ** (attempt - 1))
    logger.warning(
        "%s %s failed (%s); retry %s/%s in %.0fs",
        label,
        what,
        err,
        attempt,
        _MAX_ATTEMPTS - 1,
        delay,
    )
    return delay


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
            time.sleep(_retry_delay(label, "connect", err, attempt))


def _is_retryable_disconnect(err: sa.exc.DBAPIError, attempt: int) -> bool:
    # connection_invalidated: SQLAlchemy saw a disconnect and dropped the pool,
    # so the next checkout opens a fresh connection.
    return err.connection_invalidated and attempt < _MAX_ATTEMPTS


def retry_on_disconnect(fn: Callable[[], T], label: str) -> T:
    """Run ``fn``, retrying when the server drops the connection mid-query.

    ``pool_pre_ping`` only covers stale connections at checkout. A drop during
    a query (e.g. "SSL connection has been closed unexpectedly" while
    reflecting a table) would otherwise abort the whole pipeline.
    """
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return fn()
        except sa.exc.DBAPIError as err:
            if not _is_retryable_disconnect(err, attempt):
                raise
            time.sleep(_retry_delay(label, "query", err, attempt))
    raise AssertionError("unreachable")


async def aretry_on_disconnect(fn: Callable[[], Awaitable[T]], label: str) -> T:
    """Async variant of :func:`retry_on_disconnect`."""
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return await fn()
        except sa.exc.DBAPIError as err:
            if not _is_retryable_disconnect(err, attempt):
                raise
            await asyncio.sleep(_retry_delay(label, "query", err, attempt))
    raise AssertionError("unreachable")


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
