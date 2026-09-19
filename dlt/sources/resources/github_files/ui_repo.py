from __future__ import annotations
import logging
from typing import Any, AsyncIterator, Dict
import dlt
import re
import json5
from lib.fetch import fetch_url_json_list, fetch_url_text

logger = logging.getLogger(__name__)


async def get_github_files_ui_repo_resources() -> Any:
    """Expose GitHub UI repo resources for use by dlt pipelines."""

    @dlt.resource(
        name="beefy_platforms",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )
    async def beefy_platforms() -> AsyncIterator[Dict[str, Any]]:
        async for item in fetch_url_json_list("https://raw.githubusercontent.com/beefyfinance/beefy-v2/refs/heads/main/src/config/platforms.json"):
            yield item


    @dlt.resource(
        name="beefy_ui_chains",
        primary_key="chain_key",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )
    async def beefy_ui_chains() -> AsyncIterator[Dict[str, Any]]:
        ts_source = await fetch_url_text("https://raw.githubusercontent.com/beefyfinance/beefy-v2/refs/heads/main/src/config/config.ts")

        match = re.search(r"export const config\s*=\s*({.*});?", ts_source, re.DOTALL)
        if not match:
            raise ValueError("Could not find config object in TS file")

        object_literal = match.group(1)

        configs = json5.loads(object_literal)
        for chain_key, config in configs.items():
            yield {
                **config,
                "chain_key": chain_key,
            }

    return [beefy_platforms(), beefy_ui_chains()]
