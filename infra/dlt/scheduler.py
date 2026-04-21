#!/usr/bin/env python3
"""
Scheduler for DLT pipelines using APScheduler.
Runs three separate DLT pipelines on different schedules.
"""
from __future__ import annotations
import logging
import asyncio
import subprocess
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

TASK_TIMEOUT = 60 * 30  # 30 minutes timeout max

# Path to the dlt directory (assuming scheduler runs from /app/infra/dlt, dlt code is in /app/dlt)
DLT_DIR = Path("/app/dlt")

async def run_pipeline_script(script_name: str):
    """Run a pipeline script using uv run."""
    process = None
    try:
        logger.info(f"Starting {script_name} pipeline run...")
        # Change to the dlt directory and run the script
        # stdout=None and stderr=None let output stream directly to console
        process = await asyncio.create_subprocess_exec(
            "uv", "run", f"./{script_name}",
            cwd=str(DLT_DIR),
            stdout=None,  # Output directly to console
            stderr=None,  # Errors directly to console
        )
        
        async with asyncio.timeout(TASK_TIMEOUT):
            await process.wait()
        
        if process.returncode == 0:
            logger.info(f"{script_name} pipeline run completed successfully")
        else:
            logger.error(f"{script_name} pipeline failed with return code {process.returncode}")
    finally:
        # Kill process if it's still running (timeout or other error)
        if process and process.returncode is None:
            logger.warning(f"Killing {script_name} process...")
            try:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=30)
            except asyncio.TimeoutError:
                logger.error(f"Process {script_name} did not terminate after kill signal")

async def beefy_api_pipeline():
    """Run the beefy_api pipeline."""
    await run_pipeline_script("beefy_api_pipeline.py")

async def beefy_db_pipeline():
    """Run the beefy_db pipeline."""
    await run_pipeline_script("beefy_db_pipeline.py")

async def github_files_pipeline():
    """Run the github_files pipeline."""
    await run_pipeline_script("github_files_pipeline.py")

async def beefy_cctp_api_pipeline():
    """Run the beefy_cctp_api pipeline."""
    await run_pipeline_script("beefy_cctp_api_pipeline.py")

async def main():
    """Main async function to run the scheduler."""
    logger.info("Starting DLT scheduler with 3 pipeline tasks...")
    
    scheduler = AsyncIOScheduler()

    # Schedule beefy_api pipeline to run every 5 minutes at :00, :05, :10, etc.
    scheduler.add_job(
        beefy_api_pipeline,
        trigger=CronTrigger(minute="0/5"),
        id="beefy_api_pipeline",
        name="Beefy API Pipeline",
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,   # Combine multiple pending runs into one
    )

    # Schedule github_files pipeline to run every 5 minutes at :01, :06, :11, etc.
    scheduler.add_job(
        github_files_pipeline,
        trigger=CronTrigger(minute="1/5"),
        id="github_files_pipeline",
        name="GitHub Files Pipeline",
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,   # Combine multiple pending runs into one
    )

    # Schedule beefy_db pipeline to run every 5 minutes at :02, :07, :12, etc.
    scheduler.add_job(
        beefy_db_pipeline,
        trigger=CronTrigger(minute="2/5"),
        id="beefy_db_pipeline",
        name="Beefy DB Pipeline",
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,   # Combine multiple pending runs into one
    )

    # Schedule beefy_cctp_api pipeline to run every 5 minutes at :03, :08, :13, etc.
    scheduler.add_job(
        beefy_cctp_api_pipeline,
        trigger=CronTrigger(minute="3/5"),
        id="beefy_cctp_api_pipeline",
        name="Beefy CCTP API Pipeline",
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,   # Combine multiple pending runs into one
    )

    scheduler.start()
    
    # Keep the event loop running
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

