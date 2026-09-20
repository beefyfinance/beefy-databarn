from __future__ import annotations

import logging
import re
from typing import Mapping

import clickhouse_connect

from lib.config import CLICKHOUSE_SEND_RECEIVE_TIMEOUT, get_clickhouse_credentials

logger = logging.getLogger(__name__)

# Cache for the ClickHouse async client
_client_cache: clickhouse_connect.driver.asyncclient.AsyncClient | None = None

async def get_clickhouse_client() -> clickhouse_connect.driver.asyncclient.AsyncClient:
    """Create and return a cached ClickHouse async client from dlt credentials."""
    global _client_cache
    
    if _client_cache is None:
        credentials = get_clickhouse_credentials()
        _client_cache = await clickhouse_connect.get_async_client(
            host=credentials["host"],
            port=credentials["http_port"],
            user=credentials["user"],
            password=credentials["password"],
            database=credentials["database"],
            secure=credentials["secure"],
            send_receive_timeout=CLICKHOUSE_SEND_RECEIVE_TIMEOUT,
            settings={"send_progress_in_http_headers": 1},
        )
    
    return _client_cache


def clickhouse_default_database() -> str:
    """Get the default database from the ClickHouse credentials."""
    return get_clickhouse_credentials()["database"]

async def clickhouse_table_exists(table_name: str, database: str | None = None) -> bool:
    """
    Test if a table exists in ClickHouse.

    Args:
        table_name: Name of the table to check. Can be qualified with database (e.g. "mydb.mytable").
        database: If provided, will check in this database (overrides default in credentials).

    Returns:
        True if the table exists, False otherwise.
    """
    client = await get_clickhouse_client()
    # Determine where clause for database and table parsing
    if "." in table_name:
        db, tbl = table_name.split(".", 1)
    else:
        db = database or clickhouse_default_database()
        tbl = table_name

    query = """
        SELECT count() 
        FROM system.tables 
        WHERE database = %(database)s AND name = %(table)s
    """
    result = await client.query(query, parameters={"database": db, "table": tbl})
    count = result.result_rows[0][0] if result.result_rows else 0
    return count > 0


_HINT_WANTS_REPLACING = {
    "replacing_merge_tree": True,
    "merge_tree": False,
    "replicated_merge_tree": False,
    "shared_merge_tree": False,
}
_ENGINE_PAIRS = (
    ("ReplicatedMergeTree", "ReplicatedReplacingMergeTree"),
    ("SharedMergeTree", "SharedReplacingMergeTree"),
    ("MergeTree", "ReplacingMergeTree"),
)
_SWAP_SUFFIX = "___engine_swap"


def _sql_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ident(database: str, table: str) -> str:
    return f"`{database}`.`{table}`"


def _target_engine(current: str, want_replacing: bool) -> str | None:
    is_replacing = "ReplacingMergeTree" in current
    if is_replacing == want_replacing:
        return None
    for plain, replacing in _ENGINE_PAIRS:
        if current in (plain, replacing):
            return replacing if want_replacing else plain
    return None


def _rewrite_create_engine(
    ddl: str, database: str, table: str, swap: str, engine: str, target: str
) -> str:
    prefixes = (
        f"CREATE TABLE `{database}`.`{table}`",
        f"CREATE TABLE {database}.{table}",
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}`",
        f"CREATE TABLE IF NOT EXISTS {database}.{table}",
    )
    rewritten = None
    target_ddl = f"CREATE TABLE `{database}`.`{swap}`"
    for prefix in prefixes:
        if ddl.startswith(prefix):
            rewritten = target_ddl + ddl[len(prefix) :]
            break
    if rewritten is None:
        raise RuntimeError(f"unexpected CREATE TABLE DDL for {table}: {ddl[:160]}")
    rewritten = re.sub(r"\s+UUID\s+'[^']+'", "", rewritten, count=1)
    engine_token = f"ENGINE = {engine}"
    if engine_token not in rewritten:
        raise RuntimeError(f"{engine_token} not found in DDL for {table}")
    return rewritten.replace(engine_token, f"ENGINE = {target}", 1)


async def ensure_table_engines(dataset_name: str, table_engine_types: Mapping[str, str]) -> None:
    """Align existing tables with declared ClickHouse engine hints.

    ``replacing_merge_tree`` on a MergeTree table (or the reverse) copies rows into a
    same-named table with the other engine and ``EXCHANGE TABLES``. The live name is
    kept; the previous copy is dropped only after the exchange.
    """
    client = await get_clickhouse_client()
    database = clickhouse_default_database()
    for table, hint in table_engine_types.items():
        want_replacing = _HINT_WANTS_REPLACING.get(hint)
        if want_replacing is None:
            continue
        full_name = f"{dataset_name}___{table}"
        swap_name = f"{full_name}{_SWAP_SUFFIX}"
        swap = _ident(database, swap_name)
        await client.command(f"DROP TABLE IF EXISTS {swap}")
        meta = await client.query(
            """
            SELECT engine, create_table_query
            FROM system.tables
            WHERE database = %(database)s AND name = %(table)s
            """,
            parameters={"database": database, "table": full_name},
        )
        if not meta.result_rows:
            continue
        engine, create_sql = str(meta.result_rows[0][0]), str(meta.result_rows[0][1])
        target = _target_engine(engine, want_replacing)
        if target is None:
            continue
        dest = _ident(database, full_name)
        ddl = _rewrite_create_engine(create_sql, database, full_name, swap_name, engine, target)
        logger.info("Converting %s from %s to %s", dest, engine, target)
        await client.command(ddl)
        try:
            await client.command(f"INSERT INTO {swap} SELECT * FROM {dest}")
            await client.command(f"EXCHANGE TABLES {dest} AND {swap}")
        except Exception:
            await client.command(f"DROP TABLE IF EXISTS {swap}")
            raise
        await client.command(f"DROP TABLE IF EXISTS {swap}")


async def optimize_replacing_tables() -> None:
    """OPTIMIZE ... FINAL on dlt ReplacingMergeTree tables (names containing ``___``).

    Partitioned tables: newest partition only. Unpartitioned: the whole table.
    Intended as a periodic job, not after every pipeline load.
    """
    client = await get_clickhouse_client()
    database = clickhouse_default_database()
    tables = await client.query(
        """
        SELECT name, partition_key
        FROM system.tables
        WHERE database = %(database)s
          AND position(engine, 'ReplacingMergeTree') > 0
          AND position(name, '___') > 0
          AND NOT endsWith(name, %(swap_suffix)s)
        ORDER BY name
        """,
        parameters={"database": database, "swap_suffix": _SWAP_SUFFIX},
    )
    for full_name, partition_key in tables.result_rows:
        ident = _ident(database, str(full_name))
        if partition_key:
            parts = await client.query(
                """
                SELECT partition_id
                FROM system.parts
                WHERE database = %(database)s AND table = %(table)s AND active
                ORDER BY modification_time DESC
                LIMIT 1
                """,
                parameters={"database": database, "table": full_name},
            )
            if not parts.result_rows or not parts.result_rows[0][0]:
                continue
            sql = (
                f"OPTIMIZE TABLE {ident} PARTITION ID {_sql_str(str(parts.result_rows[0][0]))} FINAL"
            )
        else:
            sql = f"OPTIMIZE TABLE {ident} FINAL"
        logger.info("Optimizing %s", ident)
        await client.command(sql)
