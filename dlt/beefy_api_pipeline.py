from __future__ import annotations
import dlt
from sources.beefy_api import beefy_api_source
from lib.cli import run_pipeline_loop

async def main():
    pipeline = dlt.pipeline(
        pipeline_name='beefy_api',
        dev_mode=False,
        staging="filesystem",
        progress="log",
        dataset_name='beefy_api',
        destination='clickhouse',
    )

    source = await beefy_api_source()
    load_info = await run_pipeline_loop(pipeline, source)
    print(load_info)

if __name__=='__main__':
    import asyncio
    asyncio.run(main())