from __future__ import annotations

import logging
import time
from typing import Any

import psycopg2

from lib.config import get_beefy_db_url

logger = logging.getLogger(__name__)

# One try plus a few retries. Backoff doubles: 1s, 2s, 4s.
_MAX_ATTEMPTS = 4
_RETRY_BACKOFF_S = 1.0


def connect_beefy_db() -> Any:
    """Connect to Beefy DB, retrying transient libpq/SSL handshake failures.

    libpq 17+ (CVE-2024-10977) hides the postmaster error during SSL setup, so
    "too many connections" / a brief restart surfaces as "error response during
    SSL exchange". A short retry usually succeeds once a slot frees up.
    """
    url = get_beefy_db_url()
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return psycopg2.connect(url)
        except psycopg2.OperationalError as err:
            if attempt >= _MAX_ATTEMPTS:
                raise
            delay = _RETRY_BACKOFF_S * (2 ** (attempt - 1))
            logger.warning(
                "Beefy DB connect failed (%s); retry %s/%s in %.0fs",
                err,
                attempt,
                _MAX_ATTEMPTS - 1,
                delay,
            )
            time.sleep(delay)
