from __future__ import annotations

import psycopg2
import pytest

import lib.config as config
import lib.postgres as postgres


def test_with_postgres_connect_params_fills_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BEEFY_DB_SSLMODE", raising=False)
    url = config._with_postgres_connect_params(
        "postgresql://user:pass@example.com:5432/beefy-db"
    )
    assert "sslmode=require" in url
    assert "gssencmode=disable" in url
    assert "connect_timeout=10" in url


def test_with_postgres_connect_params_keeps_existing_query(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BEEFY_DB_SSLMODE", "verify-full")
    url = config._with_postgres_connect_params(
        "postgresql://user:pass@example.com:5432/beefy-db?sslmode=disable"
    )
    assert "sslmode=disable" in url
    assert "sslmode=verify-full" not in url
    assert "gssencmode=disable" in url


def test_get_beefy_db_url_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOURCES__BEEFY_DB__CREDENTIALS", raising=False)
    with pytest.raises(ValueError, match="SOURCES__BEEFY_DB__CREDENTIALS"):
        config.get_beefy_db_url()


def test_get_beefy_db_url_adds_params(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SOURCES__BEEFY_DB__CREDENTIALS",
        "postgresql://user:pass@example.com:5432/beefy-db?sslmode=require",
    )
    url = config.get_beefy_db_url()
    assert url.startswith("postgresql://user:pass@example.com:5432/beefy-db?")
    assert "sslmode=require" in url
    assert "gssencmode=disable" in url


def test_get_beefy_timescaledb_url_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOURCES__BEEFY_TIMESCALEDB__CREDENTIALS", raising=False)
    with pytest.raises(ValueError, match="SOURCES__BEEFY_TIMESCALEDB__CREDENTIALS"):
        config.get_beefy_timescaledb_url()


def test_get_beefy_timescaledb_url_uses_own_sslmode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BEEFY_DB_SSLMODE", "disable")
    monkeypatch.delenv("BEEFY_TIMESCALEDB_SSLMODE", raising=False)
    monkeypatch.setenv(
        "SOURCES__BEEFY_TIMESCALEDB__CREDENTIALS",
        "postgresql://tsdbadmin:pass@example.tsdb.cloud.timescale.com:38766/tsdb",
    )
    url = config.get_beefy_timescaledb_url()
    assert "example.tsdb.cloud.timescale.com:38766/tsdb?" in url
    assert "sslmode=require" in url
    assert "sslmode=disable" not in url
    assert "gssencmode=disable" in url


def test_connect_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    calls = {"n": 0}
    expected_url = "postgresql://user:pass@example.com:5432/beefy-db?sslmode=require"

    def connect(url: str) -> object:
        calls["n"] += 1
        assert url == expected_url
        if calls["n"] < 3:
            raise psycopg2.OperationalError(
                'connection to server failed: server sent an error response during SSL exchange'
            )
        return object()

    monkeypatch.setattr(postgres, "get_beefy_db_url", lambda: expected_url)
    monkeypatch.setattr(postgres.psycopg2, "connect", connect)
    monkeypatch.setattr(postgres.time, "sleep", sleeps.append)

    conn = postgres.connect_beefy_db()

    assert conn is not None
    assert calls["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_connect_raises_after_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    def connect(url: str) -> object:
        raise psycopg2.OperationalError("server sent an error response during SSL exchange")

    monkeypatch.setattr(postgres, "get_beefy_db_url", lambda: "postgresql://x")
    monkeypatch.setattr(postgres.psycopg2, "connect", connect)
    monkeypatch.setattr(postgres.time, "sleep", sleeps.append)

    with pytest.raises(psycopg2.OperationalError, match="SSL exchange"):
        postgres.connect_beefy_db()

    assert len(sleeps) == postgres._MAX_ATTEMPTS - 1


def test_connect_timescaledb_uses_timescaledb_url(monkeypatch: pytest.MonkeyPatch) -> None:
    expected_url = "postgresql://tsdbadmin:pass@example.tsdb.cloud.timescale.com:38766/tsdb?sslmode=require"
    seen: list[str] = []

    def connect(url: str) -> object:
        seen.append(url)
        return object()

    monkeypatch.setattr(postgres, "get_beefy_timescaledb_url", lambda: expected_url)
    monkeypatch.setattr(postgres.psycopg2, "connect", connect)

    conn = postgres.connect_beefy_timescaledb()

    assert conn is not None
    assert seen == [expected_url]
