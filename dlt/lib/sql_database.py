import logging
from typing import Any, Optional

from dlt.sources.sql_database import sql_table
from sqlalchemy.exc import NoSuchTableError

logger = logging.getLogger(__name__)


def log_missing_table(table_name: str, err: BaseException) -> None:
    logger.warning(
        "Skipping missing source table %s (%s); will retry next run",
        table_name,
        err,
    )


def try_sql_table(*, table: str, **kwargs: Any) -> Optional[Any]:
    """Call sql_table, returning None and warning if the source table is missing.

    Omitting the resource from the run leaves its dlt incremental state unchanged
    so the next pipeline run can retry once the table exists again.
    """
    try:
        return sql_table(table=table, **kwargs)
    except NoSuchTableError as e:
        log_missing_table(table, e)
        return None
