from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _cleanup():
    path = ROOT / "dlt" / "lib" / "pipeline_state_cleanup.py"
    spec_name = "pipeline_state_cleanup"
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(spec_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec_name] = module
    spec.loader.exec_module(module)
    return module


def test_obsolete_state_delete_sql_is_one_mutation():
    mod = _cleanup()
    sql = mod.obsolete_state_delete_sql("`dlt`.`beefy_db____dlt_pipeline_state`", 30, 50)
    assert sql.startswith("ALTER TABLE `dlt`.`beefy_db____dlt_pipeline_state` DELETE WHERE ")
    assert "created_at < now() - INTERVAL 30 DAY" in sql
    assert "LIMIT 50 BY pipeline_name" in sql
    assert sql.count("ALTER TABLE") == 1
    assert "SELECT pipeline_name, _dlt_load_id FROM `dlt`.`beefy_db____dlt_pipeline_state`" in sql
