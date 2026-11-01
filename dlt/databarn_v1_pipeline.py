from __future__ import annotations

import dlt

from sources.databarn_v1 import databarn_v1_source
from lib.cli import run_pipeline_loop


async def main():
    pipeline = dlt.pipeline(
        pipeline_name="databarn_v1",
        dev_mode=False,
        staging="filesystem",
        progress="log",
        dataset_name="databarn_v1",
        destination="clickhouse",
    )

    source = await databarn_v1_source()
    load_info = await run_pipeline_loop(pipeline, source)
    print(load_info)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
