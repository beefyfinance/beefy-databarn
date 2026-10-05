from __future__ import annotations

import asyncio

import httpx
import pytest

import lib.fetch as fetch
from sources.beefy_api import _optional_resource, _skip_optional_fetch


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.beefy.finance/clm-vaults")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def _fetch_error(message: str, cause: BaseException) -> fetch.FetchError:
    err = fetch.FetchError(message)
    err.__cause__ = cause
    return err


def test_skip_optional_fetch_404_and_timeout() -> None:
    timeout = _fetch_error("Failed to reach url: ReadTimeout", httpx.ReadTimeout("timed out"))
    missing = _fetch_error("Failed to reach url: HTTP 404", _http_status_error(404))
    client_error = _fetch_error("Failed to reach url: HTTP 400", _http_status_error(400))

    assert _skip_optional_fetch(timeout) is True
    assert _skip_optional_fetch(missing) is True
    assert _skip_optional_fetch(_http_status_error(404)) is True
    assert _skip_optional_fetch(client_error) is False
    assert _skip_optional_fetch(_http_status_error(400)) is False


def test_optional_resource_skips_read_timeout() -> None:
    async def factory() -> None:
        raise _fetch_error("Failed to reach url: ReadTimeout", httpx.ReadTimeout("timed out"))

    assert asyncio.run(_optional_resource("clm_vaults", factory)) is None


def test_optional_resource_reraises_client_errors() -> None:
    async def factory() -> None:
        raise _fetch_error("Failed to reach url: HTTP 400", _http_status_error(400))

    with pytest.raises(fetch.FetchError, match="HTTP 400"):
        asyncio.run(_optional_resource("clm_vaults", factory))
