#!/usr/bin/env python3
"""
Scheduler for dbt models using APScheduler.
Runs dbt models every 30 minutes, tests once per day, and regenerates docs daily (tests live in the catalog).
"""
import logging
import subprocess
import sys
import os
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import alerts
from run_tests import run_dbt_tests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def publish_docs():
    """Generate Docglow static docs. Failure must not block dbt run."""
    try:
        logger.info("Generating dbt docs (Docglow)...")
        result = subprocess.run(["/app/publish_docs.sh"], cwd="/app/dbt")
        if result.returncode == 0:
            logger.info("dbt docs published successfully")
        else:
            logger.error("dbt docs generate/publish failed (continuing without refresh)")
    except Exception as e:
        logger.error(f"Error publishing dbt docs: {e}", exc_info=True)


def run_dbt():
    """Run dbt models."""
    try:
        logger.info("Starting dbt run...")
        
        # Set working directory to dbt project
        dbt_dir = "/app/dbt"
        os.chdir(dbt_dir)
        
        os.environ["DBT_PROFILES_DIR"] = dbt_dir
        os.environ["DBT_PROJECT_DIR"] = dbt_dir
        
        # Check if dbt_packages exists and is not empty
        dbt_packages_dir = os.path.join(dbt_dir, "dbt_packages")
        if not os.path.exists(dbt_packages_dir) or not os.listdir(dbt_packages_dir):
            logger.info("dbt_packages empty, running 'dbt deps'...")
            deps_result = subprocess.run(
                ["uv", "run", "dbt", "deps"],
                cwd=dbt_dir,
            )
            if deps_result.returncode != 0:
                logger.error("Error running 'dbt deps'")
                alerts.alert_deps_failed("dbt_run", deps_result.returncode)
                return
        
        # Run dbt under lock so manual "make dbt run" and scheduler don't clash
        started = time.time()
        result = subprocess.run(
            ["/app/run_dbt_with_lock.sh", "run", "--show-all-deprecations"],
            cwd=dbt_dir,
        )
        
        if result.returncode == 0:
            logger.info("dbt run completed successfully")
        else:
            logger.error("dbt run failed")
            alerts.alert_run_failed(started, result.returncode)
            
    except Exception as e:
        logger.error(f"Error running dbt: {e}", exc_info=True)
        alerts.alert_run_exception(e)


if __name__ == "__main__":
    logger.info("Starting dbt scheduler (models every 30 minutes, tests daily at 06:00 UTC, docs daily)...")
    publish_docs()

    scheduler = BlockingScheduler()
    
    # Schedule dbt to run every 30 minutes
    scheduler.add_job(
        run_dbt,
        trigger=CronTrigger(minute="*/30"),
        id="dbt_run",
        name="dbt Run",
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,   # Combine multiple pending runs into one
    )

    scheduler.add_job(
        run_dbt_tests,
        trigger=CronTrigger(hour=6, minute=0),
        id="dbt_test",
        name="dbt Test",
        max_instances=1,
        coalesce=True,
    )

    # Docs include tests; refresh daily after a scheduled run slot
    scheduler.add_job(
        publish_docs,
        trigger=CronTrigger(hour=4, minute=15),
        id="dbt_docs",
        name="dbt Docs",
        max_instances=1,
        coalesce=True,
    )
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
        scheduler.shutdown()
