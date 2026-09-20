from __future__ import annotations

import asyncio
import logging

from lib.clickhouse import optimize_replacing_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def main() -> None:
    await optimize_replacing_tables()


if __name__ == "__main__":
    asyncio.run(main())
