from __future__ import annotations

import asyncio
import logging

from lib.pipeline_state_cleanup import cleanup_dlt_pipeline_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def main() -> None:
    await cleanup_dlt_pipeline_state()


if __name__ == "__main__":
    asyncio.run(main())
