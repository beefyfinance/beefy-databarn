from __future__ import annotations

from pathlib import Path

from sources.resources.beefy_db.tables import LOOKUP_TABLES

RESOURCES = Path(__file__).resolve().parents[1] / "sources" / "resources" / "beefy_db"
_SKIP_CREDENTIAL_CHECK = {"__init__.py", "zap_common.py"}


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


def test_beefy_db_resources_ingest_from_timescaledb() -> None:
    resource_files = [
        path for path in RESOURCES.glob("*.py") if path.name not in _SKIP_CREDENTIAL_CHECK
    ]
    assert resource_files
    for path in resource_files:
        text = path.read_text()
        assert "get_beefy_timescaledb_url" in text, path.name
        assert "get_beefy_db_url" not in text, path.name
        assert "connect_beefy_db(" not in text, path.name
