from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from lib.orphan_tables import (
    DbtExpectations,
    TableInfo,
    classify_dbt_tables,
    classify_dlt_tables,
    load_dbt_expectations,
    render_sql,
)

NOW = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
OLD = NOW - timedelta(days=90)
STALE_BEFORE = NOW - timedelta(days=30)


def table(name: str, db: str = "dlt", last_write: datetime | None = OLD) -> TableInfo:
    return TableInfo(db, name, "ReplacingMergeTree", 10, 2048, last_write)


def kinds(orphans) -> dict[str, str]:
    return {o.table.name: o.kind for o in orphans}


def test_load_dbt_expectations(tmp_path):
    manifest = {
        "nodes": {
            "model.a": {"resource_type": "model", "schema": "dbt", "alias": "stg_a", "config": {"materialized": "view"}},
            "model.eph": {"resource_type": "model", "schema": "dbt", "alias": "eph", "config": {"materialized": "ephemeral"}},
            "test.t": {"resource_type": "test", "schema": "dbt", "alias": "t", "config": {}},
        },
        "sources": {
            "source.dlt.x": {"source_name": "dlt", "identifier": "beefy_api___vaults"},
            "source.envio.y": {"source_name": "envio", "identifier": "Account"},
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    expected = load_dbt_expectations(path)
    assert expected.relations == {("dbt", "stg_a")}
    assert expected.dlt_sources == {"beefy_api___vaults"}


def test_dbt_classification():
    expected = DbtExpectations(frozenset({("analytics", "chain")}), frozenset())
    tables = [
        table("chain", "analytics"),
        table("old_mart", "analytics"),
        table("chain__dbt_backup", "analytics"),
        table("chain__dbt_tmp", "analytics", last_write=NOW - timedelta(minutes=5)),
        table("chain__dbt_new_data_c0b04d53_a6d0_4047_9ee2_e0952f6bb1a3", "analytics", last_write=None),
        table(".inner_id.abc", "analytics"),
    ]
    orphans = classify_dbt_tables(tables, expected, NOW)
    assert kinds(orphans) == {
        "old_mart": "removed-model",
        "chain__dbt_backup": "dbt-leftover",
        "chain__dbt_new_data_c0b04d53_a6d0_4047_9ee2_e0952f6bb1a3": "dbt-leftover",
    }
    assert {o.table.name: o.confidence for o in orphans}["old_mart"] == "review"


def test_dlt_classification():
    tables = [
        table("beefy_db___prices"),  # read by dbt
        table("beefy_db____dlt_loads"),
        table("beefy_db___dlt_sentinel_table"),
        table("beefy_db___apys"),  # live, recently loaded
        table("beefy_db___apys_old"),  # migration leftover
        table("beefy_db___prices___engine_swap"),
        table("beefy_db___removed_resource"),  # stale
        table("beefy_db___new_resource_new"),  # no base table: a real resource name, loaded recently
        table("beefy_api_staging___treasury"),  # main table missing
        table("beefy_api_staging___vaults"),
        table("beefy_api___vaults"),
        table("gone_pipeline___things"),
        table("handmade"),
    ]
    recent = {"beefy_db___apys": NOW, "beefy_db___new_resource_new": NOW, "beefy_api___vaults": NOW}
    orphans = classify_dlt_tables(
        tables,
        frozenset({"beefy_db", "beefy_api"}),
        frozenset({"beefy_db___prices", "beefy_db____dlt_loads"}),
        STALE_BEFORE,
        NOW,
        lambda t: recent.get(t.name),
    )
    assert kinds(orphans) == {
        "beefy_db___apys_old": "migration-leftover",
        "beefy_db___prices___engine_swap": "migration-leftover",
        "beefy_db___removed_resource": "stale-resource",
        "beefy_api_staging___treasury": "orphan-staging",
        "gone_pipeline___things": "unknown-dataset",
        "handmade": "unrecognised",
    }


def test_load_id_overrides_recent_part_time():
    """OPTIMIZE bumps part mtimes; the real last load decides staleness."""
    tables = [table("beefy_db___stale", last_write=NOW), table("beefy_db___live")]
    orphans = classify_dlt_tables(
        tables,
        frozenset({"beefy_db"}),
        frozenset(),
        STALE_BEFORE,
        NOW,
        lambda t: OLD if t.name == "beefy_db___stale" else NOW,
    )
    assert kinds(orphans) == {"beefy_db___stale": "stale-resource"}


def test_render_sql_comments_out_review_items():
    expected = DbtExpectations(frozenset({("analytics", "chain")}), frozenset())
    orphans = classify_dbt_tables(
        [table("old_mart", "analytics"), table("chain__dbt_backup", "analytics")], expected, NOW
    )
    sql = render_sql(orphans, NOW)
    lines = sql.splitlines()
    assert "DROP TABLE IF EXISTS `analytics`.`chain__dbt_backup` SYNC;" in lines
    assert "-- DROP TABLE IF EXISTS `analytics`.`old_mart` SYNC;" in lines
    assert "DROP TABLE IF EXISTS `analytics`.`old_mart` SYNC;" not in lines
