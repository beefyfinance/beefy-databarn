from __future__ import annotations

import dlt

from sources.beefy_clm_subgraph import beefy_clm_subgraph_source
from lib.cli import run_pipeline_loop


async def main():
    pipeline = dlt.pipeline(
        pipeline_name="beefy_clm_subgraph",
        dev_mode=False,
        staging="filesystem",
        progress="log",
        dataset_name="beefy_clm_subgraph",
        destination="clickhouse",
    )

    source = await beefy_clm_subgraph_source()
    load_info = await run_pipeline_loop(pipeline, source)
    print(load_info)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
