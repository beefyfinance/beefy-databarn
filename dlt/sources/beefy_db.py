# Apply ReplacingMergeTree support for ClickHouse (dlt PR #3366) before any adapter use
import lib.dlt_clickhouse_replacing_merge_tree_patch  # noqa: F401

import logging
from typing import Any
import dlt
from .resources.beefy_db.harvests import get_beefy_db_harvests_resource
from .resources.beefy_db.prices import get_beefy_db_prices_resource
from .resources.beefy_db.tvls import get_beefy_db_tvls_resource
from .resources.beefy_db.apys import get_beefy_db_apys_resource
from .resources.beefy_db.tvl_by_chain import get_beefy_db_tvl_by_chain_resource
from .resources.beefy_db.tables import get_beefy_db_other_tables_resources
from .resources.beefy_db.zap_events import get_beefy_db_zap_events_resource
from .resources.beefy_db.zap_token_transfers import get_beefy_db_zap_token_transfers_resource

logger = logging.getLogger(__name__)

@dlt.source(name="beefy_db", parallelized=True)
async def beefy_db_source() -> Any:
    """Expose Beefy DB resources for use by dlt pipelines."""

    resources = [
        await get_beefy_db_harvests_resource(),
        await get_beefy_db_prices_resource(),
        await get_beefy_db_tvls_resource(),
        await get_beefy_db_apys_resource(),
        await get_beefy_db_tvl_by_chain_resource(),
        await get_beefy_db_zap_events_resource(),
        await get_beefy_db_zap_token_transfers_resource(),
    ]
    resources.extend(get_beefy_db_other_tables_resources())

    return resources
