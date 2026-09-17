from __future__ import annotations

import json
from urllib.error import URLError

import discord_webhook as webhook


def test_inline_code_protects_dbt_double_underscores():
    assert webhook.as_inline_code("int_product_stats__lps_breakdown_hourly") == (
        "`int_product_stats__lps_breakdown_hourly`"
    )


def test_inline_code_strips_nested_backticks():
    assert webhook.as_inline_code("foo`bar") == "`foo'bar`"


def test_code_block_empty():
    assert webhook.as_code_block("  \n  ") == ""


def test_code_block_keep_start_preserves_dbt_exception():
    text = "Code: 60. DB::Exception: missing table\n" + ("SELECT 1\n" * 400)
    block = webhook.as_code_block(text, max_chars=80, keep="start")
    assert "Code: 60. DB::Exception: missing table" in block
    assert block.endswith("```")
    assert "…" in block
    assert block.index("Code: 60") < block.index("…")


def test_code_block_keep_end_preserves_traceback_tail():
    text = ("noise\n" * 200) + "Traceback (most recent call last):\nMemory limit exceeded"
    block = webhook.as_code_block(text, max_chars=80, keep="end")
    assert "Memory limit exceeded" in block
    assert block.startswith("```\n…")


def test_code_block_escapes_triple_backticks():
    assert "```" not in webhook.as_code_block("use ``` to fence").split("\n", 1)[1][:-4]


def test_notify_skips_when_webhook_unset(monkeypatch, tmp_path):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    assert webhook.notify_once_per_day("k", "t", "d", tmp_path / "state.json") is False
    assert not (tmp_path / "state.json").exists()


def test_notify_sends_once_per_utc_day(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/api/webhooks/1/abc")
    captured: dict = {}

    class Response:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    def fake_urlopen(request, timeout=10):
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data.decode())
        return Response()

    monkeypatch.setattr(webhook.urllib.request, "urlopen", fake_urlopen)
    state = tmp_path / "state.json"

    assert webhook.notify_once_per_day("beefy_api_pipeline.py", "dlt job failed", "boom", state)
    assert captured["body"]["embeds"][0]["title"] == "dlt job failed"
    assert captured["body"]["embeds"][0]["description"] == "boom"
    assert json.loads(state.read_text())["beefy_api_pipeline.py"] == webhook._today_utc()

    captured.clear()
    assert webhook.notify_once_per_day("beefy_api_pipeline.py", "dlt job failed", "boom", state) is False
    assert captured == {}


def test_notify_does_not_record_state_on_http_error(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/api/webhooks/1/abc")

    class Response:
        status = 500

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(webhook.urllib.request, "urlopen", lambda *a, **k: Response())
    state = tmp_path / "state.json"
    assert webhook.notify_once_per_day("k", "t", "d", state) is False
    assert not state.exists()


def test_notify_does_not_record_state_on_network_error(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/api/webhooks/1/abc")
    monkeypatch.setattr(
        webhook.urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(URLError("down")),
    )
    state = tmp_path / "state.json"
    assert webhook.notify_once_per_day("k", "t", "d", state) is False
    assert not state.exists()
