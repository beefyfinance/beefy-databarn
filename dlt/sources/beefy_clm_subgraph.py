from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import dlt
from dlt.common import pendulum
from dlt.destinations.adapters import clickhouse_adapter

from lib.clickhouse import get_clickhouse_client
from .resources.beefy_clm_subgraph.client import (
    fetch_subgraph_status,
    iter_all_rows,
    iter_rows_since,
)
from .resources.beefy_clm_subgraph.entities import (
    ENTITIES,
    INTEGER_FIELDS,
    TIMESTAMP_FIELDS,
    Entity,
)

logger = logging.getLogger(__name__)

# Built by dbt. beefy_key is also the subgraph slug, so every chain ingested maps to a chain_id.
CHAIN_KEYS_SQL = "SELECT beefy_key FROM analytics.chain"

_PRIMARY_KEY = ["chain_key", "id"]
# Hourly snapshots stay open for one period; re-read them, plus a margin for reorgs.
_CURSOR_LAG_S = 2 * 60 * 60
# Per resource, while the request pacer is shared: requests per run are this times the 165
# incremental resources, at 5 per second. A backfill run must finish extract, normalize and
# load inside the 30 minute scheduler timeout, or nothing loads and no cursor moves.
# Each run resumes where the last one stopped.
_MAX_PAGES_PER_RUN = 20


def _column_name(field: str) -> str:
    return field.split(" ", 1)[0]


def _columns(entity: Entity) -> Dict[str, Dict[str, Any]]:
    columns: Dict[str, Dict[str, Any]] = {
        "chain_key": {"data_type": "text", "nullable": False},
        "id": {"data_type": "text", "nullable": False},
    }
    for field in entity.fields:
        name = _column_name(field)
        if name == "id":
            continue
        if name in entity.arrays:
            columns[name] = {"data_type": "json"}
        elif name in TIMESTAMP_FIELDS:
            columns[name] = {"data_type": "timestamp"}
        elif name in INTEGER_FIELDS:
            columns[name] = {"data_type": "bigint"}
        else:
            columns[name] = {"data_type": "text"}
    return columns


def _replacing(resource: Any) -> Any:
    return clickhouse_adapter(
        resource,
        table_engine_type="replacing_merge_tree",
        sort=_PRIMARY_KEY,
        settings={"allow_nullable_key": False},
    )


def _start_timestamp(
    lagged_value: Optional[datetime], head_timestamp: Optional[datetime]
) -> int:
    """Rewind only near the subgraph head: older rows are final, and a burst of more rows
    than a run reads inside the lag window would otherwise pin a backfill on the same page.
    With the head unknown, rewind the whole lag."""
    if lagged_value is None:
        return 0
    if head_timestamp is None:
        return int(lagged_value.timestamp())
    return min(
        int(lagged_value.timestamp()) + _CURSOR_LAG_S,
        int(head_timestamp.timestamp()) - _CURSOR_LAG_S,
    )


def _entity_resource(status: Dict[str, Any], entity: Entity) -> Any:
    chain = status["chain_key"]
    # One resource per chain and entity: cursors must not be shared, chains advance
    # independently during a backfill. All chains of an entity load into one table.
    resource_name = f"{entity.table}__{chain}"
    resource_args = {
        "name": resource_name,
        "table_name": entity.table,
        "primary_key": _PRIMARY_KEY,
        "write_disposition": "append",
        "columns": _columns(entity),
    }

    if entity.cursor is None:

        @dlt.resource(**resource_args)
        async def full_refresh_rows() -> AsyncIterator[Dict[str, Any]]:
            async for row in iter_all_rows(chain, entity):
                yield row

        return _replacing(full_refresh_rows())

    @dlt.resource(**resource_args)
    async def incremental_rows(
        cursor: dlt.sources.incremental[Any] = dlt.sources.incremental(
            entity.cursor,
            initial_value=None,
            # Rewritten snapshots keep their id and cursor value: dlt must not drop them
            # as duplicates, ReplacingMergeTree keeps the latest.
            primary_key=(),
            lag=_CURSOR_LAG_S,
            last_value_func=max,
            row_order="asc",
        ),
    ) -> AsyncIterator[Dict[str, Any]]:
        last_value: Optional[datetime] = cursor.last_value
        start_timestamp = _start_timestamp(last_value, status["head_block_timestamp"])
        async for row in iter_rows_since(
            chain, entity, start_timestamp, max_pages=_MAX_PAGES_PER_RUN
        ):
            yield row

    return _replacing(incremental_rows())


def _status_resource(statuses: List[Dict[str, Any]]) -> Any:
    @dlt.resource(
        name="subgraph_status",
        primary_key=["chain_key", "loaded_at"],
        write_disposition="append",
        columns={
            "chain_key": {"data_type": "text", "nullable": False},
            "loaded_at": {"data_type": "timestamp", "nullable": False},
            "deployment": {"data_type": "text"},
            "has_indexing_errors": {"data_type": "bool"},
            "head_block_number": {"data_type": "bigint"},
            "head_block_timestamp": {"data_type": "timestamp"},
        },
    )
    def subgraph_status() -> Any:
        loaded_at = pendulum.now("UTC")
        for status in statuses:
            yield {**status, "loaded_at": loaded_at}

    return subgraph_status()


async def _fetch_chain_keys() -> List[str]:
    client = await get_clickhouse_client()
    result = await client.query(CHAIN_KEYS_SQL)
    chain_keys = sorted(row[0] for row in result.result_rows)
    if not chain_keys:
        raise RuntimeError("No chains in analytics.chain; run dbt first")
    return chain_keys


async def _fetch_deployed_statuses() -> List[Dict[str, Any]]:
    """Return the status of every Beefy chain that has a beefy-clm subgraph."""
    statuses: List[Dict[str, Any]] = []
    for chain in await _fetch_chain_keys():
        status = await fetch_subgraph_status(chain)
        if status is None:
            logger.info("No beefy-clm subgraph on %s; skipping chain", chain)
            continue
        if status["has_indexing_errors"]:
            logger.warning("beefy-clm subgraph on %s reports indexing errors", chain)
        if status["head_block_timestamp"] is None:
            logger.warning("beefy-clm subgraph on %s reports no head block timestamp", chain)
        statuses.append(status)
    return statuses


@dlt.source(name="beefy_clm_subgraph", parallelized=True, max_table_nesting=0)
async def beefy_clm_subgraph_source() -> Any:
    statuses = await _fetch_deployed_statuses()
    resources = [
        _entity_resource(status, entity)
        for status in statuses
        for entity in ENTITIES
    ]
    return [_status_resource(statuses), *resources]
