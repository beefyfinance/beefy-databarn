from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

import lib.fetch as fetch


class _ScriptedClient:
    def __init__(self, script: list[Any]) -> None:
        self._script = script

    async def __aenter__(self) -> "_ScriptedClient":
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        if isinstance(item, tuple):
            status, content = item
        else:
            status, content = item, b"{}"
        request = httpx.Request("GET", url)
        return httpx.Response(status, request=request, content=content)


def _install(monkeypatch: pytest.MonkeyPatch, script: list[Any], sleeps: list[float]) -> None:
    def factory(*args: Any, **kwargs: Any) -> _ScriptedClient:
        return _ScriptedClient(script)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(fetch.httpx, "AsyncClient", factory)
    monkeypatch.setattr(fetch.asyncio, "sleep", sleep)


def test_read_timeout_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    script: list[Any] = [httpx.ReadTimeout("timed out"), httpx.ReadTimeout("timed out"), 200]
    _install(monkeypatch, script, sleeps)

    response = asyncio.run(fetch._get("https://api.beefy.finance/clm-vaults"))

    assert response.status_code == 200
    assert sleeps == [1.0, 2.0]
    assert script == []


def test_read_timeout_raises_after_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    script: list[Any] = [httpx.ReadTimeout("timed out")] * fetch._MAX_ATTEMPTS
    _install(monkeypatch, script, sleeps)

    with pytest.raises(fetch.FetchError, match="ReadTimeout"):
        asyncio.run(fetch._get("https://api.beefy.finance/clm-vaults"))

    assert len(sleeps) == fetch._MAX_ATTEMPTS - 1
    assert script == []


def test_client_error_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    script: list[Any] = [404]
    _install(monkeypatch, script, sleeps)

    with pytest.raises(fetch.FetchError, match="HTTP 404"):
        asyncio.run(fetch._get("https://api.beefy.finance/clm-vaults"))

    assert sleeps == []


def test_get_default_timeout_allows_large_json_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def factory(*args: Any, **kwargs: Any) -> _ScriptedClient:
        seen.update(kwargs)
        return _ScriptedClient([200])

    monkeypatch.setattr(fetch.httpx, "AsyncClient", factory)

    asyncio.run(fetch._get("https://api.beefy.finance/clm-vaults"))

    assert seen["timeout"] == fetch._DEFAULT_TIMEOUT_S
    assert fetch._DEFAULT_TIMEOUT_S == 30.0


def test_server_error_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    script: list[Any] = [503, 200]
    _install(monkeypatch, script, sleeps)

    response = asyncio.run(fetch._get("https://api.beefy.finance/clm-vaults"))

    assert response.status_code == 200
    assert sleeps == [1.0]
    assert script == []


def test_http_500_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    script: list[Any] = [500, 500, 500, 500, 200]
    _install(monkeypatch, script, sleeps)

    response = asyncio.run(fetch._get("https://cctp-relay.beefy.com/v1/messages"))

    assert response.status_code == 200
    assert sleeps == [1.0, 2.0, 4.0, 8.0]
    assert script == []


def test_http_500_raises_after_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    body = b'{"error":"relay exploded","code":500}'
    script: list[Any] = [(500, body)] * fetch._MAX_ATTEMPTS
    _install(monkeypatch, script, sleeps)

    with pytest.raises(fetch.FetchError, match=r'HTTP 500 Internal Server Error: \{"error":"relay exploded"') as raised:
        asyncio.run(fetch._get("https://cctp-relay.beefy.com/v1/messages"))

    assert "Failed to reach https://cctp-relay.beefy.com/v1/messages" in str(raised.value)
    assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 32.0]
    assert script == []


def test_response_snippet_collapses_and_truncates() -> None:
    request = httpx.Request("GET", "https://cctp-relay.beefy.com/v1/messages")
    response = httpx.Response(
        500,
        request=request,
        content=b"{\n  \"error\": \"down\"\n}\n",
    )
    assert fetch._response_snippet(response) == '{ "error": "down" }'

    huge = "x" * 800
    long_response = httpx.Response(500, request=request, content=huge.encode())
    snippet = fetch._response_snippet(long_response)
    assert snippet.endswith("…")
    assert len(snippet) == fetch._RESPONSE_SNIPPET_CHARS
