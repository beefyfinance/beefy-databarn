from __future__ import annotations

from sources.resources.beefy_db.tables import TIMESCALEDB_TABLES


def test_all_lookup_tables_come_from_timescaledb() -> None:
    assert set(TIMESCALEDB_TABLES) == {
        "address_metadata",
        "bifi_buyback",
        "vault_strategies",
        "feebatch_harvests",
        "chains",
        "price_oracles",
        "vault_ids",
    }
