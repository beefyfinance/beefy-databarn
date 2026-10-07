import logging
import re
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, Optional, Set

import sqlalchemy as sa
from dlt.sources.sql_database import sql_table
from sqlalchemy.exc import NoSuchTableError

logger = logging.getLogger(__name__)

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_ident(name: str) -> str:
    if not _IDENT.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier {name!r}")
    return name


def time_bounded_select(
    table: Any,
    *,
    time_column: str,
    start_value: datetime,
    end_value: datetime,
    any_filters: Optional[Mapping[str, tuple[str, Any]]] = None,
) -> Any:
    """SELECT * with a closed time window so Timescale can exclude chunks.

    ``any_filters`` maps a column name to ``(bind_param, values)`` and emits
    ``column = ANY(:bind_param)``.
    """
    time_column = _require_ident(time_column)
    clauses = []
    params: dict[str, Any] = {
        "start_value": start_value,
        "end_value": end_value,
    }
    for column, (param, values) in (any_filters or {}).items():
        clauses.append(f"{_require_ident(column)} = ANY(:{_require_ident(param)})")
        params[param] = values
    clauses.append(f"{time_column} > :start_value")
    clauses.append(f"{time_column} <= :end_value")
    return sa.text(
        f"SELECT * FROM {table.fullname} WHERE " + " AND ".join(clauses)
    ).bindparams(**params)


def time_window_bounds(
    incremental: Any,
    *,
    default_start: datetime,
    window_days: int,
) -> tuple[datetime, datetime]:
    """Inclusive-end window from the incremental cursor (or genesis)."""
    start_value = None if incremental is None else incremental.start_value
    if start_value is None:
        start_value = default_start
    return start_value, start_value + timedelta(days=window_days)


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
