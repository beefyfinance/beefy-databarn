from __future__ import annotations

from typing import Any

import dlt

from .resources.beefy_cctp_api.messages import get_beefy_cctp_api_messages_resource


@dlt.source(name="beefy_cctp_api", parallelized=True, max_table_nesting=0)
async def beefy_cctp_api_source() -> Any:
    return [await get_beefy_cctp_api_messages_resource()]

