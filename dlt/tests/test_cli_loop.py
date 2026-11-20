from __future__ import annotations

from types import SimpleNamespace

from lib.cli import _incremental_state


def _pipeline(state: object) -> SimpleNamespace:
    return SimpleNamespace(state=state)


def test_incremental_state_reads_the_resource_cursor() -> None:
    state = {"sources": {"databarn_v1": {"resources": {"vault_stats": {"incremental": {"datetime": {"last_value": 1}}}}}}}
    assert _incremental_state(_pipeline(state), "databarn_v1", "vault_stats") == repr({"datetime": {"last_value": 1}})


def test_incremental_state_changes_with_the_cursor() -> None:
    def state(last: int) -> dict:
        return {"sources": {"s": {"resources": {"r": {"incremental": {"datetime": {"last_value": last}}}}}}}

    assert _incremental_state(_pipeline(state(1)), "s", "r") != _incremental_state(_pipeline(state(2)), "s", "r")


def test_incremental_state_is_none_without_a_cursor() -> None:
    assert _incremental_state(_pipeline({}), "s", "r") is None
    assert _incremental_state(_pipeline({"sources": {"s": {"resources": {"r": {}}}}}), "s", "r") is None
