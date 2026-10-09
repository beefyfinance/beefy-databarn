from __future__ import annotations

from sources.resources.beefy_db.tables import HEROKU_TABLES, TIMESCALEDB_TABLES


def test_migrated_lookups_come_from_timescaledb() -> None:
    assert set(TIMESCALEDB_TABLES) == {"chains", "price_oracles", "vault_ids"}
    assert set(HEROKU_TABLES) == {
        "address_metadata",
        "bifi_buyback",
        "vault_strategies",
        "feebatch_harvests",
    }
    assert set(TIMESCALEDB_TABLES).isdisjoint(HEROKU_TABLES)
