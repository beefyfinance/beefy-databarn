from __future__ import annotations

import pytest

from sources.resources.github_files.ui_repo import iter_ui_chain_records, parse_ui_chain_configs

CURRENT_CONFIG_TS = """
import { mapValues } from 'lodash-es';

const featureDefaultsByStatus = {
  active: { price: true, apy: true },
} as const satisfies Record<string, object>;

const chainConfigInputs = {
  ethereum: {
    status: 'active',
    name: 'Ethereum',
    chainId: 1,
    new: true,
  },
  moonbeam: {
    status: 'eol',
    retiredAt: 1764838421,
    name: 'Moonbeam',
    chainId: 1284,
  },
  heco: {
    status: 'disabled',
    retiredAt: 1681913494,
    name: 'HECO',
    chainId: 128,
  },
} satisfies Record<string, object>;

export const config = mapValues(chainConfigInputs, input => input);
"""

LEGACY_CONFIG_TS = """
export const config = {
  ethereum: {
    name: 'Ethereum',
    chainId: 1,
    new: true,
  },
  moonbeam: {
    name: 'Moonbeam',
    chainId: 1284,
    eol: 1764838421,
  },
  heco: {
    name: 'HECO',
    chainId: 128,
    eol: 1681913494,
    disabled: true,
  },
};
"""


def _by_key(ts_source: str) -> dict[str, dict]:
    return {row["chain_key"]: row for row in iter_ui_chain_records(ts_source)}


def test_parse_current_chain_config_inputs() -> None:
    configs = parse_ui_chain_configs(CURRENT_CONFIG_TS)
    assert set(configs) == {"ethereum", "moonbeam", "heco"}
    assert configs["heco"]["status"] == "disabled"
    assert configs["moonbeam"]["retiredAt"] == 1764838421


def test_current_format_keeps_eol_disabled_aliases() -> None:
    rows = _by_key(CURRENT_CONFIG_TS)

    assert "eol" not in rows["ethereum"]
    assert "disabled" not in rows["ethereum"]
    assert rows["ethereum"]["status"] == "active"
    assert rows["ethereum"]["new"] is True

    assert rows["moonbeam"]["eol"] == 1764838421
    assert "disabled" not in rows["moonbeam"]
    assert rows["moonbeam"]["status"] == "eol"

    assert rows["heco"]["eol"] == 1681913494
    assert rows["heco"]["disabled"] is True
    assert rows["heco"]["status"] == "disabled"


def test_legacy_export_const_config_still_parses() -> None:
    rows = _by_key(LEGACY_CONFIG_TS)
    assert rows["ethereum"]["chainId"] == 1
    assert "eol" not in rows["ethereum"]
    assert rows["moonbeam"]["eol"] == 1764838421
    assert "disabled" not in rows["moonbeam"]
    assert rows["heco"]["disabled"] is True


def test_missing_config_object_raises() -> None:
    with pytest.raises(ValueError, match="Could not find config object in TS file"):
        parse_ui_chain_configs("export const config = mapValues(other, x => x);")
