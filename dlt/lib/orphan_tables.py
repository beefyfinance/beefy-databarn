"""Find ClickHouse tables that no code produces or reads any more (read-only).

Nothing here ever runs DDL. `find_orphans` only SELECTs from `system.*` (plus
`max(_dlt_load_id)` on candidate dlt tables); `render_sql` returns text for a
human to review and run by hand.

Sources of truth for "what should exist":
- dbt databases (`dbt`, `analytics`): every model/seed/snapshot in `target/manifest.json`.
- dlt database: dataset names from `dlt/*_pipeline.py`, plus the tables dbt reads via `source('dlt', ...)`.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Literal

DBT_LEFTOVER_SUFFIXES = ("__dbt_tmp", "__dbt_backup", "__dbt_new_data")
# dlt/.migrations/*.sql and ensure_table_engines leave these behind.
DLT_LEFTOVER_SUFFIXES = ("___engine_swap", "_old", "_new", "_bak", "_backup", "_tmp")
DLT_INTERNAL_TABLES = frozenset({"_dlt_loads", "_dlt_version", "_dlt_pipeline_state", "dlt_sentinel_table"})
DLT_STAGING_SUFFIX = "_staging"
IN_PROGRESS_WINDOW = timedelta(hours=1)

Confidence = Literal["high", "review"]


@dataclass(frozen=True)
class TableInfo:
    database: str
    name: str
    engine: str
    rows: int
    bytes: int
    last_write: datetime | None


@dataclass(frozen=True)
class Orphan:
    table: TableInfo
    kind: str
    reason: str
    confidence: Confidence


@dataclass(frozen=True)
class DbtExpectations:
    relations: frozenset[tuple[str, str]]  # (schema, alias) of models, seeds, snapshots
    dlt_sources: frozenset[str]  # table names dbt reads through source('dlt', ...)


def load_dbt_expectations(manifest_path: Path) -> DbtExpectations:
    manifest = json.loads(manifest_path.read_text())
    relations = {
        (node["schema"], node["alias"])
        for node in manifest["nodes"].values()
        if node["resource_type"] in ("model", "seed", "snapshot")
        and node["config"].get("materialized") != "ephemeral"
    }
    dlt_sources = {
        source["identifier"]
        for source in manifest.get("sources", {}).values()
        if source["source_name"] == "dlt"
    }
    return DbtExpectations(frozenset(relations), frozenset(dlt_sources))


def dlt_dataset_names(dlt_dir: Path) -> frozenset[str]:
    pattern = re.compile(r"""dataset_name\s*=\s*['"]([^'"]+)['"]""")
    names = {m for path in dlt_dir.glob("*_pipeline.py") for m in pattern.findall(path.read_text())}
    return frozenset(names)


def manifest_is_stale(manifest_path: Path, project_dir: Path) -> bool:
    """True when a model/macro/yaml file changed after the manifest was written."""
    manifest_mtime = manifest_path.stat().st_mtime
    for folder in ("models", "macros", "seeds"):
        for path in (project_dir / folder).rglob("*"):
            if path.is_file() and path.suffix in (".sql", ".yml", ".yaml", ".csv") and path.stat().st_mtime > manifest_mtime:
                return True
    return False


def classify_dbt_tables(tables: Iterable[TableInfo], expected: DbtExpectations, now: datetime) -> list[Orphan]:
    orphans = []
    for table in tables:
        if table.name.startswith(".") or (table.database, table.name) in expected.relations:
            continue
        if table.name.endswith(DBT_LEFTOVER_SUFFIXES):
            if _written_within(table, now, IN_PROGRESS_WINDOW):
                continue  # a dbt run may be using it right now
            orphans.append(Orphan(table, "dbt-leftover", "temporary/backup relation from an interrupted dbt run", "high"))
        else:
            orphans.append(
                Orphan(table, "removed-model", "not produced by any dbt model, seed or snapshot (removed or renamed?)", "review")
            )
    return orphans


def classify_dlt_tables(
    tables: list[TableInfo],
    datasets: frozenset[str],
    referenced: frozenset[str],
    stale_before: datetime,
    now: datetime,
    refine_last_write: Callable[[TableInfo], datetime | None],
) -> list[Orphan]:
    existing = {t.name for t in tables}
    by_longest_dataset = sorted(datasets, key=len, reverse=True)
    orphans = []
    for table in tables:
        name = table.name
        if name.startswith(".") or name in referenced:
            continue
        dataset, staging, rest = _split_dlt_name(name, by_longest_dataset)
        if dataset is None:
            if "___" in name:
                orphans.append(Orphan(table, "unknown-dataset", "dataset prefix matches no dlt/*_pipeline.py (pipeline removed or renamed?)", "review"))
            else:
                orphans.append(Orphan(table, "unrecognised", "not a dlt table name (`<dataset>___<table>`)", "review"))
            continue
        if staging:
            if f"{dataset}___{rest}" not in existing:
                orphans.append(Orphan(table, "orphan-staging", "dlt staging copy whose main table no longer exists", "high"))
            continue
        if rest in DLT_INTERNAL_TABLES:
            continue
        suffix = next((s for s in DLT_LEFTOVER_SUFFIXES if rest.endswith(s) and len(rest) > len(s)), None)
        if suffix and (suffix == "___engine_swap" or f"{dataset}___{rest[: -len(suffix)]}" in existing):
            if _written_within(table, now, IN_PROGRESS_WINDOW):
                continue
            orphans.append(Orphan(table, "migration-leftover", f"`{suffix}` copy next to the live table (manual migration or engine swap)", "high"))
            continue
        last_write = refine_last_write(table) or table.last_write
        if last_write is not None and last_write >= stale_before:
            continue
        stamp = last_write.date().isoformat() if last_write else "never"
        orphans.append(
            Orphan(
                TableInfo(table.database, name, table.engine, table.rows, table.bytes, last_write),
                "stale-resource",
                f"not read by dbt and last loaded {stamp} (resource removed from the source?)",
                "review",
            )
        )
    return orphans


def _split_dlt_name(name: str, datasets_longest_first: list[str]) -> tuple[str | None, bool, str]:
    for dataset in datasets_longest_first:
        for staging in (False, True):
            prefix = f"{dataset}{DLT_STAGING_SUFFIX if staging else ''}___"
            if name.startswith(prefix):
                return dataset, staging, name[len(prefix) :]
    return None, False, name


def _written_within(table: TableInfo, now: datetime, window: timedelta) -> bool:
    return table.last_write is not None and now - table.last_write < window


def _ident(database: str, table: str) -> str:
    return "`{}`.`{}`".format(database.replace("`", "``"), table.replace("`", "``"))


def format_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def render_report(orphans: list[Orphan]) -> str:
    if not orphans:
        return "No orphan tables found."
    lines = []
    for orphan in sorted(orphans, key=lambda o: (o.table.database, o.confidence != "high", o.kind, o.table.name)):
        t = orphan.table
        written = t.last_write.strftime("%Y-%m-%d") if t.last_write else "-"
        lines.append(
            f"{orphan.confidence:<6} {orphan.kind:<18} {t.database}.{t.name}\n"
            f"       {t.engine}, {t.rows:,} rows, {format_bytes(t.bytes)}, last write {written}: {orphan.reason}"
        )
    total = sum(o.table.bytes for o in orphans)
    lines.append(f"\n{len(orphans)} tables, {format_bytes(total)} total")
    return "\n".join(lines)


def render_sql(orphans: list[Orphan], generated_at: datetime) -> str:
    header = [
        "-- Generated by `make orphans sql` on " + generated_at.strftime("%Y-%m-%d %H:%M UTC"),
        "-- NOT executed automatically. Review every line, then run the ones you want as a ClickHouse admin.",
        "-- Do not run while dbt or dlt is running (leftover tables may be in use).",
        "-- `high`: temporary/leftover tables, active below. `review`: commented out, uncomment one by one.",
        "-- If dbt data was lost on purpose, remember ClickHouse has no undo: consider `make clickhouse backup` first.",
        "",
    ]
    body = []
    for orphan in sorted(orphans, key=lambda o: (o.table.database, o.confidence != "high", o.kind, o.table.name)):
        t = orphan.table
        comment = f"-- [{orphan.confidence}] {orphan.kind}: {orphan.reason} ({t.engine}, {t.rows:,} rows, {format_bytes(t.bytes)})"
        statement = f"DROP TABLE IF EXISTS {_ident(t.database, t.name)} SYNC;"
        body.append(comment)
        body.append(statement if orphan.confidence == "high" else f"-- {statement}")
        body.append("")
    return "\n".join(header + body)


# --- ClickHouse access (read-only) -------------------------------------------------


def _client(host: str, port: int, user: str, password: str, secure: bool = False):
    import clickhouse_connect

    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password, secure=secure)


_TABLES_WITH_PARTS_SQL = """
    SELECT t.name, t.engine, coalesce(t.total_rows, 0), coalesce(t.total_bytes, 0),
           coalesce(p.last_part_write, t.metadata_modification_time)
    FROM system.tables AS t
    LEFT JOIN (
        SELECT table, max(modification_time) AS last_part_write
        FROM system.parts
        WHERE database = %(database)s AND active
        GROUP BY table
    ) AS p ON p.table = t.name
    WHERE t.database = %(database)s AND NOT t.is_temporary
    ORDER BY t.name
"""
_TABLES_SQL = """
    SELECT name, engine, coalesce(total_rows, 0), coalesce(total_bytes, 0), metadata_modification_time
    FROM system.tables
    WHERE database = %(database)s AND NOT is_temporary
    ORDER BY name
"""


def list_tables(client, database: str) -> list[TableInfo]:
    """Last write comes from system.parts when the user may read it, else from table metadata."""
    parameters = {"database": database}
    try:
        result = client.query(_TABLES_WITH_PARTS_SQL, parameters=parameters)
    except Exception as exc:
        if "ACCESS_DENIED" not in str(exc):
            raise
        result = client.query(_TABLES_SQL, parameters=parameters)
    return [
        TableInfo(database, str(name), str(engine), int(rows), int(size), _utc(last_write))
        for name, engine, rows, size, last_write in result.result_rows
    ]


def last_dlt_load(client, table: TableInfo) -> datetime | None:
    """Newest `_dlt_load_id` (unix seconds as text) in the table; None if it has no such column."""
    try:
        result = client.query(
            f"SELECT max(toFloat64OrZero(_dlt_load_id)) FROM {_ident(table.database, table.name)}"
        )
    except Exception:
        return None
    seconds = result.result_rows[0][0] if result.result_rows else 0
    return datetime.fromtimestamp(seconds, tz=timezone.utc) if seconds else None


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def find_orphans(dbt_dir: Path, dlt_dir: Path, manifest_path: Path, stale_days: int) -> list[Orphan]:
    from lib.config import get_clickhouse_credentials

    now = datetime.now(timezone.utc)
    expected = load_dbt_expectations(manifest_path)
    orphans: list[Orphan] = []

    dbt_client = _client(
        os.environ.get("DBT_CLICKHOUSE_HOST", "localhost"),
        int(os.environ.get("DBT_CLICKHOUSE_PORT", "8123")),
        os.environ.get("DBT_CLICKHOUSE_USER", "dbt"),
        os.environ["DBT_CLICKHOUSE_PASSWORD"],
    )
    for database in sorted({schema for schema, _ in expected.relations}):
        orphans += classify_dbt_tables(list_tables(dbt_client, database), expected, now)

    creds = get_clickhouse_credentials()
    dlt_client = _client(creds["host"], creds["http_port"], creds["user"], creds["password"], bool(creds["secure"]))
    orphans += classify_dlt_tables(
        list_tables(dlt_client, creds["database"]),
        dlt_dataset_names(dlt_dir),
        expected.dlt_sources,
        now - timedelta(days=stale_days),
        now,
        lambda table: last_dlt_load(dlt_client, table),
    )
    return orphans
