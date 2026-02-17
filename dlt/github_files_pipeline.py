from __future__ import annotations
import dlt
from sources.github_files import github_files_source
from lib.cli import run_pipeline_loop

async def main():
    pipeline = dlt.pipeline(
        pipeline_name='github_files',
        dev_mode=False,
        staging="filesystem",
        progress="log",
        dataset_name='github_files',
        destination='clickhouse',
    )

    source = await github_files_source()
    load_info = await run_pipeline_loop(pipeline, source)
    print(load_info)

if __name__=='__main__':
    import asyncio
    asyncio.run(main())