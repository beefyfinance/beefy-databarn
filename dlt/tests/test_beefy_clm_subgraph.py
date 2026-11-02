from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from lib.fetch import FetchError
import sources.beefy_clm_subgraph as source
from sources.resources.beefy_clm_subgraph import client
from sources.resources.beefy_clm_subgraph.entities import ENTITIES, Entity

EVENT = Entity(table="events", query="events", cursor="timestamp", fields=("id", "timestamp"))
SNAPSHOT = Entity(
    table="snapshots",
    query="snapshots",
    cursor="roundedTimestamp",
    where="period: 3600",
    fields=("id", "clm { id }", "roundedTimestamp", "blockNumber"),
)


def _install_pages(monkeypatch: pytest.MonkeyPatch, pages: list[list[dict[str, Any]]]) -> list[str]:
    queries: list[str] = []

    async def query_subgraph(chain: str, query: str) -> dict[str, Any]:
        queries.append(query)
        return {"rows": pages.pop(0)}

    monkeypatch.setattr(client, "query_subgraph", query_subgraph)
    return queries


def _collect(chain: str, entity: Entity, start: int, max_pages: int = 10) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        return [r async for r in client.iter_rows_since(chain, entity, start, max_pages=max_pages)]

    return asyncio.run(run())


def _full_page(timestamps: list[int]) -> list[dict[str, Any]]:
    return [{"id": f"0x{i:04x}", "timestamp": str(ts)} for i, ts in enumerate(timestamps)]


def test_page_restarts_at_last_cursor_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client, "PAGE_SIZE", 2)
    queries = _install_pages(
        monkeypatch, [_full_page([100, 200]), _full_page([200])]
    )

    rows = _collect("base", EVENT, 100)

    assert len(rows) == 3
    assert "timestamp_gte: 100" in queries[0]
    assert "timestamp_gte: 200" in queries[1]


def test_page_of_one_cursor_value_is_read_by_id_then_moves_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client, "PAGE_SIZE", 2)
    queries = _install_pages(
        monkeypatch,
        [
            _full_page([100, 100]),
            [{"id": "0x0009", "timestamp": "100"}],
            _full_page([101]),
        ],
    )

    rows = _collect("base", EVENT, 100)

    assert len(rows) == 4
    assert 'timestamp: 100, id_gt: "0x0001"' in queries[1]
    assert "orderBy: id" in queries[1]
    assert "timestamp_gte: 101" in queries[2]


def test_max_pages_stops_a_backfill_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client, "PAGE_SIZE", 2)
    queries = _install_pages(
        monkeypatch, [_full_page([100, 200]), _full_page([200, 300]), _full_page([300, 400])]
    )

    rows = _collect("base", EVENT, 100, max_pages=2)

    assert len(queries) == 2
    assert len(rows) == 4


def test_rows_are_flattened(monkeypatch: pytest.MonkeyPatch) -> None:
    queries = _install_pages(
        monkeypatch,
        [[
            {
                "id": "0xa",
                "clm": {"id": "0xb"},
                "roundedTimestamp": "3600",
                "blockNumber": "42",
            }
        ]],
    )

    rows = _collect("base", SNAPSHOT, 0)

    assert rows == [
        {
            "chain_key": "base",
            "id": "0xa",
            "clm": "0xb",
            "roundedTimestamp": datetime(1970, 1, 1, 1, tzinfo=timezone.utc),
            "blockNumber": 42,
        }
    ]
    assert "period: 3600, roundedTimestamp_gte: 0" in queries[0]


def test_iter_all_rows_pages_by_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client, "PAGE_SIZE", 2)
    entity = Entity(table="t", query="tokens", fields=("id",))
    queries = _install_pages(
        monkeypatch, [[{"id": "0x1"}, {"id": "0x2"}], [{"id": "0x3"}]]
    )

    async def run() -> list[dict[str, Any]]:
        return [row async for row in client.iter_all_rows("base", entity)]

    assert [row["id"] for row in asyncio.run(run())] == ["0x1", "0x2", "0x3"]
    assert 'id_gt: "0x2"' in queries[1]


def test_graphql_errors_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    async def post(url: str, *, json_body: dict[str, Any]) -> dict[str, Any]:
        return {"errors": [{"message": "boom"}]}

    monkeypatch.setattr(client, "post_url_json_dict", post)

    with pytest.raises(client.SubgraphError, match="boom"):
        asyncio.run(client.query_subgraph("base", "{ x }"))


REORG_ERROR = {"errors": [{"message": "the chain was reorganized while executing the query"}]}


def test_reorg_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client, "_REORG_BACKOFF_S", 0)
    payloads = [REORG_ERROR, REORG_ERROR, {"data": {"rows": []}}]

    async def post(url: str, *, json_body: dict[str, Any]) -> dict[str, Any]:
        return payloads.pop(0)

    monkeypatch.setattr(client, "post_url_json_dict", post)

    assert asyncio.run(client.query_subgraph("rootstock", "{ x }")) == {"rows": []}
    assert payloads == []


def test_reorg_retries_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client, "_REORG_BACKOFF_S", 0)
    calls = 0

    async def post(url: str, *, json_body: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return REORG_ERROR

    monkeypatch.setattr(client, "post_url_json_dict", post)

    with pytest.raises(client.SubgraphError, match="reorganized"):
        asyncio.run(client.query_subgraph("rootstock", "{ x }"))
    assert calls == client._REORG_MAX_ATTEMPTS


def test_missing_subgraph_is_none_and_other_failures_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    def failure(status: int) -> FetchError:
        request = httpx.Request("POST", "https://example.test")
        response = httpx.Response(status, request=request)
        error = FetchError("failed")
        error.__cause__ = httpx.HTTPStatusError("failed", request=request, response=response)
        return error

    statuses = [404, 500]

    async def post(url: str, *, json_body: dict[str, Any]) -> dict[str, Any]:
        raise failure(statuses.pop(0))

    monkeypatch.setattr(client, "post_url_json_dict", post)

    assert asyncio.run(client.fetch_subgraph_status("gnosis")) is None
    with pytest.raises(FetchError):
        asyncio.run(client.fetch_subgraph_status("gnosis"))


def _install_chain_keys(monkeypatch: pytest.MonkeyPatch, chain_keys: list[str]) -> None:
    class Client:
        async def query(self, sql: str) -> SimpleNamespace:
            return SimpleNamespace(result_rows=[(key,) for key in chain_keys])

    async def get_client() -> Client:
        return Client()

    monkeypatch.setattr(source, "get_clickhouse_client", get_client)


def test_only_chains_with_a_deployment_are_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_chain_keys(monkeypatch, ["gnosis", "base", "arbitrum"])

    async def status(chain: str) -> dict[str, Any] | None:
        if chain == "gnosis":
            return None
        return {"chain_key": chain, "has_indexing_errors": False}

    monkeypatch.setattr(source, "fetch_subgraph_status", status)

    statuses = asyncio.run(source._fetch_deployed_statuses())

    assert [row["chain_key"] for row in statuses] == ["arbitrum", "base"]


def test_empty_chain_table_fails_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_chain_keys(monkeypatch, [])

    with pytest.raises(RuntimeError, match="run dbt first"):
        asyncio.run(source._fetch_deployed_statuses())


def test_entity_arrays_are_selected_fields() -> None:
    for entity in ENTITIES:
        selected = {field.split(" ", 1)[0] for field in entity.fields}
        assert set(entity.arrays) <= selected, entity.table
        assert "id" in selected
        assert entity.cursor is None or entity.cursor in selected


def test_start_timestamp_only_rewinds_near_the_head() -> None:
    head = datetime(2026, 1, 2, tzinfo=timezone.utc)
    lag = source._CURSOR_LAG_S
    head_ts = int(head.timestamp())

    assert source._start_timestamp(None, head) == 0

    # Backfilling: resume at the high-water mark, dlt's lagged value + lag.
    behind = datetime.fromtimestamp(head_ts - 10 * lag, tz=timezone.utc)
    assert source._start_timestamp(behind, head) == head_ts - 9 * lag

    # Caught up: re-read the open window.
    caught_up = datetime.fromtimestamp(head_ts - lag, tz=timezone.utc)
    assert source._start_timestamp(caught_up, head) == head_ts - lag
