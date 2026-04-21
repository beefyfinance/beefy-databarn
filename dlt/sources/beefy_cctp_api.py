from __future__ import annotations

# Apply ReplacingMergeTree support for ClickHouse (dlt PR #3366) before any adapter use
import lib.dlt_clickhouse_replacing_merge_tree_patch  # noqa: F401

from typing import Any

import dlt

from .resources.beefy_cctp_api.messages import get_beefy_cctp_api_messages_resource


@dlt.source(name="beefy_cctp_api", parallelized=True, max_table_nesting=0)
async def beefy_cctp_api_source() -> Any:
    return [await get_beefy_cctp_api_messages_resource()]

