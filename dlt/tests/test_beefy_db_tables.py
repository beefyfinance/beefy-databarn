from __future__ import annotations

from sources.resources.beefy_db.tables import LOOKUP_TABLES


def test_lookup_tables_cover_all_small_beefy_db_tables() -> None:
    assert set(LOOKUP_TABLES) == {
        "address_metadata",
        "bifi_buyback",
        "vault_strategies",
        "feebatch_harvests",
        "chains",
        "price_oracles",
        "vault_ids",
    }
