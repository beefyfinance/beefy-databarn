from __future__ import annotations
import dlt
from sources.beefy_db import beefy_db_source
from lib.cli import run_pipeline_loop
from lib.config import BEEFY_TIMESCALEDB_POOL_SIZE

async def main():
    pipeline = dlt.pipeline(
        pipeline_name='beefy_db',
        dev_mode=False,
        staging="filesystem",
        progress="log",
        dataset_name='beefy_db',
        destination='clickhouse',
    )

    source = await beefy_db_source()
    # Parallel extract, capped at the Timescale role connection budget.
    load_info = await run_pipeline_loop(
        pipeline,
        source,
        extract_workers=BEEFY_TIMESCALEDB_POOL_SIZE,
        extract_max_parallel_items=BEEFY_TIMESCALEDB_POOL_SIZE,
    )
    print(load_info)

if __name__=='__main__':
    import asyncio
    asyncio.run(main())