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
        status = self._script.pop(0)
        if isinstance(status, Exception):
            raise status
        request = httpx.Request("GET", url)
        return httpx.Response(status, request=request, content=b"{}")


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


def test_server_error_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    script: list[Any] = [503, 200]
    _install(monkeypatch, script, sleeps)

    response = asyncio.run(fetch._get("https://api.beefy.finance/clm-vaults"))

    assert response.status_code == 200
    assert sleeps == [1.0]
    assert script == []
