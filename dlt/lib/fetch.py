import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Optional, Tuple, Mapping
import httpx

logger = logging.getLogger(__name__)

# One try plus a few retries. Backoff doubles: 1s, 2s, 4s.
_MAX_ATTEMPTS = 4
_RETRY_BACKOFF_S = 1.0
_RETRY_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class FetchError(RuntimeError):
    """A URL could not be fetched."""


def _unreachable(url: str, exc: httpx.HTTPError) -> FetchError:
    target = url
    try:
        request = exc.request
    except RuntimeError:
        request = None
    if request is None and isinstance(exc, httpx.HTTPStatusError):
        request = exc.response.request
    if request is not None:
        target = str(request.url)

    if isinstance(exc, httpx.HTTPStatusError):
        reason = exc.response.reason_phrase or ""
        detail = f"HTTP {exc.response.status_code} {reason}".rstrip()
    else:
        message = str(exc).strip()
        name = type(exc).__name__
        detail = f"{name}: {message}" if message and message != name else name
    return FetchError(f"Failed to reach {target}: {detail}")


def _retryable(exc: httpx.HTTPError) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRY_STATUS_CODES
    return False


async def _get(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout_s: float = 5.0,
) -> httpx.Response:
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    exc: httpx.HTTPError
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(limits=limits, timeout=timeout_s) as client:
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                await response.aread()
                return response
        except httpx.HTTPError as err:
            exc = err
            if attempt >= _MAX_ATTEMPTS or not _retryable(err):
                break
            delay = _RETRY_BACKOFF_S * (2 ** (attempt - 1))
            logger.warning(
                "Request to %s failed (%s); retry %s/%s in %.0fs",
                url,
                type(err).__name__,
                attempt,
                _MAX_ATTEMPTS - 1,
                delay,
            )
            await asyncio.sleep(delay)
    raise _unreachable(url, exc) from exc


async def fetch_url_text(url: str) -> str:
    response = await _get(url)
    return response.text

async def _fetch_url_json(url: str) -> Tuple[Any, Optional[str]]:
    """Fetch a URL and return the JSON payload and its ETag, if any."""
    response = await _get(url)
    payload = response.json()
    etag = response.headers.get("etag")
    return payload, etag


async def fetch_url_json_dict_with_params(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout_s: float = 30.0,
) -> Tuple[Dict[str, Any], Optional[str]]:
    response = await _get(url, params=params, headers=headers, timeout_s=timeout_s)
    payload = response.json()
    etag = response.headers.get("etag")
    if not isinstance(payload, dict):
        raise ValueError("Unexpected JSON payload; expected a dict.")
    return payload, etag

async def fetch_url_json_list(url: str) -> AsyncIterator[Dict[str, Any]]:
    payload, _ = await _fetch_url_json(url)
    if not isinstance(payload, list):
        raise ValueError("Unexpected Beefy API payload; expected a list.")

    for item in payload:
        yield item

async def fetch_url_json_dict(url: str) -> Tuple[Dict[str, Any], Optional[str]]:
    payload, etag = await _fetch_url_json(url)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Beefy API payload; expected a dict.")
    return payload, etag
    