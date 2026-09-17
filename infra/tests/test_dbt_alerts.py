from __future__ import annotations

import json
from pathlib import Path

DB_ERROR = (
    "Database Error in model int_product_stats__lps_breakdown_hourly "
    "(models/intermediate/int_product_stats__lps_breakdown_hourly.sql)\n"
    "  Code: 497. DB::Exception: int_product_stats__lps_breakdown_hourly: "
    "Not enough privileges. To execute this query, it's necessary to have "
    "grant SELECT ON analytics.foo.\n"
    "  compiled Code at target/run/beefy_databarn/models/intermediate/"
    "int_product_stats__lps_breakdown_hourly.sql\n"
    "  SELECT * FROM huge_compiled_dump " + ("x" * 4000)
)


def _write_run_results(path: Path, results: list[dict]) -> Path:
    path.write_text(json.dumps({"results": results}))
    return path


def test_display_name_strips_project_and_test_hash(dbt_alerts):
    assert (
        dbt_alerts._display_name("model.beefy_databarn.int_product_stats__lps_breakdown_hourly")
        == "int_product_stats__lps_breakdown_hourly"
    )
    assert (
        dbt_alerts._display_name("test.beefy_databarn.not_null_foo." + "a" * 32)
        == "not_null_foo"
    )


def test_compact_message_keeps_exception_and_drops_compiled_sql(dbt_alerts):
    compact = dbt_alerts._compact_dbt_message(DB_ERROR)
    assert "Not enough privileges" in compact
    assert "compiled Code at" not in compact
    assert "huge_compiled_dump" not in compact


def test_summarize_includes_full_database_error_not_first_line_only(dbt_alerts, tmp_path):
    path = _write_run_results(
        tmp_path / "run_results.json",
        [
            {
                "status": "error",
                "unique_id": "model.beefy_databarn.int_product_stats__lps_breakdown_hourly",
                "message": DB_ERROR,
            }
        ],
    )
    text = dbt_alerts._summarize_run_results(path)
    assert text.startswith("1 failed\n")
    assert "`int_product_stats__lps_breakdown_hourly`" in text
    assert "Not enough privileges" in text
    assert "grant SELECT ON analytics.foo" in text
    assert "huge_compiled_dump" not in text
    # Discord would eat __lps__ if the name were not in backticks.
    unquoted = text.replace("`int_product_stats__lps_breakdown_hourly`", "")
    assert "• int_product_stats__lps_breakdown_hourly" not in unquoted


def test_summarize_test_failures_include_row_counts(dbt_alerts, tmp_path):
    path = _write_run_results(
        tmp_path / "run_results.json",
        [
            {
                "status": "fail",
                "unique_id": "test.beefy_databarn.not_null_foo." + "b" * 32,
                "failures": 12,
                "message": "Got 12 results, configured to fail if != 0",
            }
        ],
    )
    text = dbt_alerts._summarize_run_results(path)
    assert "`not_null_foo` (12 rows)" in text
    assert "Got 12 results" in text


def test_summarize_missing_and_empty_results(dbt_alerts, tmp_path):
    assert dbt_alerts._summarize_run_results(tmp_path / "missing.json") == (
        "Could not read dbt run_results.json."
    )
    path = _write_run_results(
        tmp_path / "run_results.json",
        [{"status": "success", "unique_id": "model.beefy_databarn.ok"}],
    )
    assert "no failed nodes" in dbt_alerts._summarize_run_results(path)


def test_summarize_caps_items(dbt_alerts, tmp_path):
    results = [
        {
            "status": "error",
            "unique_id": f"model.beefy_databarn.model_{i}",
            "message": f"Database Error in model model_{i}\n  boom {i}",
        }
        for i in range(12)
    ]
    path = _write_run_results(tmp_path / "run_results.json", results)
    text = dbt_alerts._summarize_run_results(path, max_items=3)
    assert text.startswith("12 failed\n")
    assert "… and 9 more" in text
    assert "`model_0`" in text
    assert "`model_3`" not in text


def test_failure_description_uses_run_results_when_fresh(dbt_alerts, tmp_path, monkeypatch):
    path = _write_run_results(
        tmp_path / "run_results.json",
        [
            {
                "status": "error",
                "unique_id": "model.beefy_databarn.int_product_stats__lps_breakdown_hourly",
                "message": DB_ERROR,
            }
        ],
    )
    monkeypatch.setattr(dbt_alerts, "RUN_RESULTS_PATH", path)
    desc = dbt_alerts._failure_description(path.stat().st_mtime, 1, "run")
    assert "Not enough privileges" in desc


def test_failure_description_without_results(dbt_alerts, tmp_path, monkeypatch):
    monkeypatch.setattr(dbt_alerts, "RUN_RESULTS_PATH", tmp_path / "missing.json")
    desc = dbt_alerts._failure_description(0, 2, "run")
    assert "exit code 2" in desc
    assert "did not write run_results.json" in desc


def test_alert_run_failed_sends_database_error(dbt_alerts, tmp_path, monkeypatch):
    path = _write_run_results(
        tmp_path / "run_results.json",
        [
            {
                "status": "error",
                "unique_id": "model.beefy_databarn.int_product_stats__lps_breakdown_hourly",
                "message": DB_ERROR,
            }
        ],
    )
    monkeypatch.setattr(dbt_alerts, "RUN_RESULTS_PATH", path)
    monkeypatch.setattr(dbt_alerts, "ALERT_STATE_PATH", tmp_path / "state.json")
    sent: list[tuple] = []
    monkeypatch.setattr(
        dbt_alerts,
        "notify_once_per_day",
        lambda key, title, description, state_path, **kwargs: sent.append(
            (key, title, description)
        )
        or True,
    )

    dbt_alerts.alert_run_failed(path.stat().st_mtime, 1)

    assert sent == [
        (
            "dbt_run",
            "dbt run failed (prod)",
            dbt_alerts._summarize_run_results(path),
        )
    ]
    assert "Not enough privileges" in sent[0][2]
