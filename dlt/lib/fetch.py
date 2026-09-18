from typing import Any, AsyncIterator, Dict, Optional, Tuple, Mapping
import httpx


class FetchError(RuntimeError):
    """A URL could not be fetched."""


def _unreachable(url: str, exc: httpx.HTTPError) -> FetchError:
    target = url
    request = getattr(exc, "request", None)
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


async def _get(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout_s: float = 5.0,
) -> httpx.Response:
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    try:
        async with httpx.AsyncClient(limits=limits, timeout=timeout_s) as client:
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            await response.aread()
            return response
    except httpx.HTTPError as exc:
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
    