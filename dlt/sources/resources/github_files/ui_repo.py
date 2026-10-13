from __future__ import annotations
import logging
from typing import Any, AsyncIterator, Dict, Iterator, Mapping
import dlt
import re
import json5
from dlt.destinations.adapters import clickhouse_adapter
from lib.fetch import fetch_url_json_list, fetch_url_text

logger = logging.getLogger(__name__)

# beefy-v2 config.ts shapes:
# - current: `const chainConfigInputs = { ... } satisfies Record<...>`
# - legacy:  `export const config = { ... };`
_CHAIN_CONFIG_OBJECT_PATTERNS = (
    re.compile(r"const chainConfigInputs\s*=\s*({.*?})\s*satisfies", re.DOTALL),
    re.compile(r"export const config\s*=\s*({.*});?", re.DOTALL),
)


def parse_ui_chain_configs(ts_source: str) -> Dict[str, Dict[str, Any]]:
    """Extract the chain config object literal from beefy-v2 config.ts."""
    object_literal: str | None = None
    for pattern in _CHAIN_CONFIG_OBJECT_PATTERNS:
        match = pattern.search(ts_source)
        if match:
            object_literal = match.group(1)
            break
    if object_literal is None:
        raise ValueError("Could not find config object in TS file")

    configs = json5.loads(object_literal)
    if not isinstance(configs, dict) or not configs:
        raise ValueError("Could not find config object in TS file")
    return configs


def iter_ui_chain_records(ts_source: str) -> Iterator[Dict[str, Any]]:
    """Yield one row per chain, including eol/disabled aliases for dbt staging."""
    for chain_key, config in parse_ui_chain_configs(ts_source).items():
        if not isinstance(config, Mapping):
            raise ValueError(f"Invalid chain config for {chain_key}")
        yield normalize_ui_chain_record(chain_key, config)


def normalize_ui_chain_record(chain_key: str, config: Mapping[str, Any]) -> Dict[str, Any]:
    """Map status/retiredAt onto the legacy eol/disabled columns dbt already reads."""
    record = dict(config)
    status = record.get("status")
    if "eol" not in record and record.get("retiredAt") is not None:
        record["eol"] = record["retiredAt"]
    if "disabled" not in record and status == "disabled":
        record["disabled"] = True
    return {**record, "chain_key": chain_key}


async def get_github_files_ui_repo_resources() -> Any:
    """Expose GitHub UI repo resources for use by dlt pipelines."""

    @dlt.resource(
        name="beefy_platforms",
        primary_key="id",
        write_disposition="append",
    )
    async def beefy_platforms() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://raw.githubusercontent.com/beefyfinance/beefy-v2/refs/heads/main/src/config/platforms.json"):
            yield item


    @dlt.resource(
        name="beefy_ui_chains",
        primary_key="chain_key",
        write_disposition="append",
    )
    async def beefy_ui_chains() -> AsyncIterator[Dict[str, Any]]:
        ts_source = await fetch_url_text("https://raw.githubusercontent.com/beefyfinance/beefy-v2/refs/heads/main/src/config/config.ts")
        for record in iter_ui_chain_records(ts_source):
            yield record

    return [
        clickhouse_adapter(beefy_platforms(), table_engine_type="replacing_merge_tree"),
        clickhouse_adapter(beefy_ui_chains(), table_engine_type="replacing_merge_tree"),
    ]
