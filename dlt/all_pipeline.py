import asyncio
from beefy_api_pipeline import main as beefy_api_pipeline_main
from beefy_db_pipeline import main as beefy_db_pipeline_main
from github_files_pipeline import main as github_files_pipeline_main

async def main():
    print("Running all pipelines ...")
    await asyncio.gather(
        beefy_api_pipeline_main(),
        github_files_pipeline_main(),
        beefy_db_pipeline_main()
    )

if __name__=='__main__':
    asyncio.run(main())