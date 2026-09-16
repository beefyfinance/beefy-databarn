"""Daily dbt test run (invoked by the scheduler)."""
import logging
import os
import subprocess
import time

import alerts

logger = logging.getLogger(__name__)


def run_dbt_tests():
    """Run dbt tests once per day."""
    try:
        logger.info("Starting dbt test...")
        env = os.environ.copy()
        env["DBT_RUN_LOCK_TIMEOUT"] = "1200"
        started = time.time()
        result = subprocess.run(
            ["/app/run_dbt_with_lock.sh", "test"],
            env=env,
        )

        if result.returncode == 0:
            logger.info("dbt test completed successfully")
        else:
            logger.error("dbt test failed")
            alerts.alert_test_failed(started, result.returncode)
    except Exception as e:
        logger.error(f"Error running dbt tests: {e}", exc_info=True)
        alerts.alert_test_exception(e)
