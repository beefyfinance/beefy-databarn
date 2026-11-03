"""GraphQL paging over Goldsky-hosted subgraphs."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx

from lib.fetch import FetchError, RequestPacer, post_url_json_dict
from .entities import INTEGER_FIELDS, TIMESTAMP_FIELDS, Entity

logger = logging.getLogger(__name__)

GOLDSKY_BASE_URL = (
    "https://api.goldsky.com/api/public/project_clu2walwem1qm01w40v3yhw1f/subgraphs"
)
SUBGRAPH_NAME = "beefy-clm"
PAGE_SIZE = 1000
# Goldsky public endpoints allow 50 requests per 10 seconds.
MIN_REQUEST_INTERVAL_S = 0.2
_ID_START = "0x"
# graph-node aborts a query when a reorg lands mid-execution; the same query then succeeds.
# One try plus retries, backoff doubling: 1s, 2s, 4s, 8s, 16s.
_REORG_ERROR = "chain was reorganized"
_REORG_MAX_ATTEMPTS = 6
_REORG_BACKOFF_S = 1.0


class SubgraphError(RuntimeError):
    """A subgraph answered with GraphQL errors."""


_pacer = RequestPacer(MIN_REQUEST_INTERVAL_S)


def subgraph_url(chain: str) -> str:
    return f"{GOLDSKY_BASE_URL}/{SUBGRAPH_NAME}-{chain}/latest/gn"


async def query_subgraph(chain: str, query: str) -> Dict[str, Any]:
    attempt = 0
    while True:
        attempt += 1
        await _pacer.wait()
        payload = await post_url_json_dict(subgraph_url(chain), json_body={"query": query})
        # graph-node reports query failures as HTTP 200.
        errors = payload.get("errors")
        if not errors:
            return payload["data"]
        if attempt >= _REORG_MAX_ATTEMPTS or _REORG_ERROR not in str(errors):
            raise SubgraphError(f"{SUBGRAPH_NAME}-{chain}: {errors}")
        delay = _REORG_BACKOFF_S * (2 ** (attempt - 1))
        logger.warning(
            "%s-%s reorged during query; retry %s/%s in %.0fs",
            SUBGRAPH_NAME,
            chain,
            attempt,
            _REORG_MAX_ATTEMPTS - 1,
            delay,
        )
        await asyncio.sleep(delay)


async def fetch_subgraph_status(chain: str) -> Optional[Dict[str, Any]]:
    """Return indexing status, or None when the subgraph is not deployed on this chain."""
    try:
        data = await query_subgraph(
            chain, "{ _meta { deployment hasIndexingErrors block { number timestamp } } }"
        )
    except FetchError as exc:
        cause = exc.__cause__
        if isinstance(cause, httpx.HTTPStatusError) and cause.response.status_code == 404:
            return None
        raise
    meta = data["_meta"]
    # graph-node answers a null timestamp while it has not cached the head block yet.
    head_timestamp = meta["block"]["timestamp"]
    return {
        "chain_key": chain,
        "deployment": meta["deployment"],
        "has_indexing_errors": meta["hasIndexingErrors"],
        "head_block_number": meta["block"]["number"],
        "head_block_timestamp": None if head_timestamp is None else _to_datetime(head_timestamp),
    }


def _build_query(entity: Entity, *, order_by: str, where: str) -> str:
    filters = ", ".join(part for part in (entity.where, where) if part)
    return (
        f"{{ rows: {entity.query}(first: {PAGE_SIZE}, orderBy: {order_by}, orderDirection: asc, "
        f"where: {{ {filters} }}) {{ {' '.join(entity.fields)} }} }}"
    )


def _to_datetime(value: Any) -> datetime:
    return datetime.fromtimestamp(int(value), tz=timezone.utc)


def _flatten_row(chain: str, row: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {"chain_key": chain}
    for name, value in row.items():
        if isinstance(value, dict):
            value = value["id"]
        elif name in TIMESTAMP_FIELDS:
            value = _to_datetime(value)
        elif name in INTEGER_FIELDS:
            value = int(value)
        flat[name] = value
    return flat


async def _fetch_rows(
    chain: str, entity: Entity, *, order_by: str, where: str
) -> List[Dict[str, Any]]:
    data = await query_subgraph(chain, _build_query(entity, order_by=order_by, where=where))
    return data["rows"]


async def iter_all_rows(chain: str, entity: Entity) -> AsyncIterator[Dict[str, Any]]:
    """Read a whole entity in id order."""
    last_id = _ID_START
    while True:
        rows = await _fetch_rows(chain, entity, order_by="id", where=f'id_gt: "{last_id}"')
        for row in rows:
            yield _flatten_row(chain, row)
        if len(rows) < PAGE_SIZE:
            return
        last_id = rows[-1]["id"]


async def iter_rows_since(
    chain: str, entity: Entity, start_timestamp: int, *, max_pages: int
) -> AsyncIterator[Dict[str, Any]]:
    """Read rows from start_timestamp in cursor order, for at most max_pages requests.

    Each page restarts at the last cursor value it saw, so rows sharing that value are
    re-read and collapse in ReplacingMergeTree. A page holding a single cursor value cannot
    advance that way, so that value is read in id order before moving on.
    """
    assert entity.cursor is not None
    cursor = entity.cursor
    position = start_timestamp
    pages = 0
    while pages < max_pages:
        rows = await _fetch_rows(chain, entity, order_by=cursor, where=f"{cursor}_gte: {position}")
        pages += 1
        for row in rows:
            yield _flatten_row(chain, row)
        if len(rows) < PAGE_SIZE:
            return

        last_position = int(rows[-1][cursor])
        if last_position > position:
            position = last_position
            continue

        last_id = rows[-1]["id"]
        while len(rows) == PAGE_SIZE:
            rows = await _fetch_rows(
                chain, entity, order_by="id", where=f'{cursor}: {position}, id_gt: "{last_id}"'
            )
            pages += 1
            for row in rows:
                yield _flatten_row(chain, row)
            if rows:
                last_id = rows[-1]["id"]
        position += 1
