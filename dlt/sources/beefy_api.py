from __future__ import annotations
import logging
from typing import Any, Awaitable, Callable, Optional

import dlt
import httpx

from lib.fetch import FetchError
from .resources.beefy_api.configs import (
    get_beefy_api_boosts_resource,
    get_beefy_api_clm_vaults_resource,
    get_beefy_api_cow_vaults_resource,
    get_beefy_api_gov_vaults_resource,
    get_beefy_api_tokens_resource,
    get_beefy_api_vaults_resource,
)
from .resources.beefy_api.snapshots import (
    get_beefy_api_apy_breakdown_resource,
    get_beefy_api_apy_resource,
    get_beefy_api_lps_breakdown_resource,
    get_beefy_api_lps_resource,
    get_beefy_api_mootokenprices_resource,
    get_beefy_api_prices_resource,
    get_beefy_api_treasury_resource,
    get_beefy_api_tvl_resource,
)

logger = logging.getLogger(__name__)


def _skip_optional_fetch(exc: BaseException) -> bool:
    # _get wraps httpx errors in FetchError; 404 and exhausted retries for
    # timeouts/network must not abort the rest of the source.
    if isinstance(exc, FetchError):
        cause = exc.__cause__
        if cause is not None:
            return _skip_optional_fetch(cause)
        return "HTTP 404" in str(exc)
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 404
    return isinstance(
        exc,
        (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError),
    )


async def _optional_resource(
    name: str, factory: Callable[[], Awaitable[Any]]
) -> Optional[Any]:
    # Soft-skip missing or unreachable API endpoints so other resources still load.
    # Omitting the resource keeps its dlt state unchanged for the next run.
    try:
        return await factory()
    except (FetchError, httpx.HTTPError) as e:
        if not _skip_optional_fetch(e):
            raise
        logger.warning(
            "Skipping API resource %s (%s); will retry next run",
            name,
            e,
        )
        return None


@dlt.source(
    name="beefy_api",
    max_table_nesting=0,
    parallelized=True,
)
async def beefy_api_source() -> Any:
    """Expose Beefy Config API resources for use by dlt pipelines."""

    candidates = [
        await _optional_resource("vaults", get_beefy_api_vaults_resource),
        await _optional_resource("gov_vaults", get_beefy_api_gov_vaults_resource),
        await _optional_resource("clm_vaults", get_beefy_api_clm_vaults_resource),
        await _optional_resource("cow_vaults", get_beefy_api_cow_vaults_resource),
        await _optional_resource("boosts", get_beefy_api_boosts_resource),
        await _optional_resource("tokens", get_beefy_api_tokens_resource),
        await _optional_resource("apy", get_beefy_api_apy_resource),
        await _optional_resource("lps", get_beefy_api_lps_resource),
        await _optional_resource("prices", get_beefy_api_prices_resource),
        await _optional_resource("lps_breakdown", get_beefy_api_lps_breakdown_resource),
        await _optional_resource("apy_breakdown", get_beefy_api_apy_breakdown_resource),
        await _optional_resource("tvl", get_beefy_api_tvl_resource),
        await _optional_resource("mootokenprices", get_beefy_api_mootokenprices_resource),
        await _optional_resource("treasury", get_beefy_api_treasury_resource),
    ]
    return [r for r in candidates if r is not None]
