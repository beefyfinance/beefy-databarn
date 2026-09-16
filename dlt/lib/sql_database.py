import logging
from typing import Any, Callable, Optional, Set

import sqlalchemy as sa
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


def compose_query_adapters(
    *adapters: Callable[..., Any],
) -> Callable[..., Any]:
    """Apply query adapters left-to-right, each seeing the previous rewrite."""

    def query_adapter_callback(query, table, incremental=None, engine=None):
        for adapter in adapters:
            query = adapter(query, table, incremental=incremental, engine=engine)
        return query

    return query_adapter_callback


def hex_encode_bytea_columns(
    column_names: Set[str],
) -> Callable[..., Any]:
    """Return a query_adapter_callback that hex-encodes Postgres bytea columns.

    Emits ``'0x' || encode(<col>, 'hex') AS <col>`` so pyarrow can load the
    values as UTF-8 text. Preserves the dlt-built query's WHERE/ORDER (e.g.
    incremental filters).
    """

    def query_adapter_callback(query, table, incremental=None, engine=None):
        columns = []
        for col in query.selected_columns:
            if col.name in column_names:
                columns.append(
                    (
                        sa.literal("0x").op("||")(
                            sa.func.encode(table.c[col.name], "hex")
                        )
                    ).label(col.name)
                )
            else:
                columns.append(col)
        return query.with_only_columns(*columns)

    return query_adapter_callback


def postgres_array_as_json_text(
    column_names: Set[str],
) -> Callable[..., Any]:
    """Serialize Postgres arrays to JSON text (``["a","b"]``) in SQL.

    Avoids Arrow nested→JSON fallbacks. Downstream dbt ``to_str_list`` already
    JSONExtracts these strings.
    """

    def query_adapter_callback(query, table, incremental=None, engine=None):
        columns = []
        for col in query.selected_columns:
            if col.name in column_names:
                columns.append(
                    sa.cast(sa.func.array_to_json(table.c[col.name]), sa.Text).label(
                        col.name
                    )
                )
            else:
                columns.append(col)
        return query.with_only_columns(*columns)

    return query_adapter_callback
