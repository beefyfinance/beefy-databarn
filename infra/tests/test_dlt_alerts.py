from __future__ import annotations

TRACEBACK_LOG = """
2026-09-17 18:03:01 [INFO] Starting beefy_db_pipeline.py
progress 10%\rprogress 50%\rprogress 100%
2026-09-17 18:03:02 [INFO] loading harvests
\x1b[31mTraceback (most recent call last):\x1b[0m
  File "./beefy_db_pipeline.py", line 17, in <module>
    asyncio.run(main())
  File "/usr/lib/python3.13/asyncio/runners.py", line 194, in run
    return runner.run(main)
clickhouse_driver.errors.ServerException: Code: 241. Memory limit exceeded: would use 32.00 GiB
"""

ERROR_LOG = "\n".join(
    ["2026-09-17 18:03:01 [INFO] Starting beefy_api_pipeline.py"]
    + [f"ok {i}" for i in range(80)]
    + [
        "2026-09-17 18:03:02 [ERROR] Resource harvests failed to load",
        "clickhouse_connect.driver.exceptions.DatabaseError: HTTPDriver returned response code 500",
    ]
)


def test_excerpt_keeps_exception_at_end_of_long_traceback(dlt_alerts):
    frames = "\n".join(
        f'  File "/app/site-packages/httpx/frame_{i}.py", line 1, in send' for i in range(400)
    )
    log = (
        "Traceback (most recent call last):\n"
        + frames
        + "\nlib.fetch.FetchError: Failed to reach https://api.beefy.finance/vaults: ConnectTimeout: timed out\n"
    )
    excerpt = dlt_alerts._log_excerpt(log)
    assert "Failed to reach https://api.beefy.finance/vaults: ConnectTimeout: timed out" in excerpt
    assert "frame_0.py" not in excerpt


def test_alert_job_failed_surfaces_unreachable_url(dlt_alerts, monkeypatch):
    sent: list[tuple] = []
    monkeypatch.setattr(
        dlt_alerts,
        "notify_once_per_day",
        lambda key, title, description, state_path, **kwargs: sent.append(description) or True,
    )
    log = """
Traceback (most recent call last):
  File "/app/dlt/lib/fetch.py", line 16, in _fetch_url_json
    response = await client.get(url)
lib.fetch.FetchError: Failed to reach https://api.beefy.finance:443/vaults: ConnectTimeout: timed out
"""
    dlt_alerts.alert_job_failed("beefy_api_pipeline.py", 1, log)

    description = sent[0]
    assert "exited with return code 1" in description
    assert "Failed to reach `https://api.beefy.finance:443/vaults`: ConnectTimeout: timed out" in description
    assert description.index("Failed to reach") < description.index("```")


def test_excerpt_keeps_traceback_not_startup_logs(dlt_alerts):
    excerpt = dlt_alerts._log_excerpt(TRACEBACK_LOG)
    assert excerpt.startswith("Traceback (most recent call last):")
    assert "Memory limit exceeded" in excerpt
    assert "Starting beefy_db_pipeline.py" not in excerpt
    assert "\x1b[" not in excerpt


def test_excerpt_keeps_error_line_from_long_success_output(dlt_alerts):
    excerpt = dlt_alerts._log_excerpt(ERROR_LOG)
    assert "[ERROR] Resource harvests failed to load" in excerpt
    assert "response code 500" in excerpt
    assert "Starting beefy_api_pipeline.py" not in excerpt
    assert "ok 0" not in excerpt


def test_excerpt_empty_and_fallback_tail(dlt_alerts):
    assert dlt_alerts._log_excerpt("   ") == ""
    lines = [f"line {i}" for i in range(80)]
    excerpt = dlt_alerts._log_excerpt("\n".join(lines))
    assert "line 0" not in excerpt
    assert "line 79" in excerpt
    assert len(excerpt.splitlines()) == 40


def test_with_output_wraps_excerpt_in_code_block(dlt_alerts):
    prefix = "Pipeline `beefy_db_pipeline.py` exited with return code 1."
    assert dlt_alerts._with_output(prefix, "") == prefix
    text = dlt_alerts._with_output(prefix, TRACEBACK_LOG)
    assert text.startswith(prefix + "\n\n```\nTraceback")
    assert "Memory limit exceeded" in text
    assert text.endswith("```")


def test_alert_job_failed_includes_return_code_and_traceback(dlt_alerts, monkeypatch, tmp_path):
    sent: list[tuple] = []
    monkeypatch.setattr(dlt_alerts, "ALERT_STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        dlt_alerts,
        "notify_once_per_day",
        lambda key, title, description, state_path, **kwargs: sent.append(
            (key, title, description)
        )
        or True,
    )

    dlt_alerts.alert_job_failed("beefy_db_pipeline.py", 1, TRACEBACK_LOG)

    key, title, description = sent[0]
    assert key == "beefy_db_pipeline.py"
    assert title == "dlt job failed: beefy_db_pipeline.py"
    assert "exited with return code 1" in description
    assert "`beefy_db_pipeline.py`" in description
    assert "Memory limit exceeded" in description
    assert "Starting beefy_db_pipeline.py" not in description


def test_alert_job_timeout_includes_last_logs(dlt_alerts, monkeypatch, tmp_path):
    sent: list[tuple] = []
    monkeypatch.setattr(
        dlt_alerts,
        "notify_once_per_day",
        lambda key, title, description, state_path, **kwargs: sent.append(description) or True,
    )

    dlt_alerts.alert_job_timeout("beefy_api_pipeline.py", 1800, ERROR_LOG)

    assert "30-minute timeout" in sent[0]
    assert "response code 500" in sent[0]


def test_alert_job_error_includes_exception(dlt_alerts, monkeypatch):
    sent: list[tuple] = []
    monkeypatch.setattr(
        dlt_alerts,
        "notify_once_per_day",
        lambda key, title, description, state_path, **kwargs: sent.append(
            (title, description)
        )
        or True,
    )

    dlt_alerts.alert_job_error("beefy_cctp_api_pipeline.py", RuntimeError("disk full"))

    title, description = sent[0]
    assert title == "dlt job error: beefy_cctp_api_pipeline.py"
    assert "RuntimeError: disk full" in description
