"""Avoid ClickHouse OOM when dlt restores pipeline state.

dlt's default get_stored_state is:

    SELECT ... FROM _dlt_pipeline_state AS s
    JOIN _dlt_loads AS l ON l.load_id = s._dlt_load_id
    WHERE pipeline_name = %s AND l.status = 0
    ORDER BY load_id DESC LIMIT 1

On large append-only metadata tables ClickHouse hash-joins the right side in
memory (FillingRightJoinSide). The state column is a compressed blob copied on
every run, so that join can exceed max_memory_usage (Code 241).

Tables created before dlt 1.26 also use ORDER BY tuple(), so the planner cannot
prune. Same lookup as a semi-join / IN subquery keeps the fat strings off the
hash table.
"""

from dlt.common.destination.client import StateInfo
from dlt.common.pendulum import pendulum
from dlt.common.schema.typing import C_DLT_LOAD_ID, C_DLT_LOADS_TABLE_LOAD_ID
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient

_ORIGINAL_GET_STORED_STATE = ClickHouseClient.get_stored_state


def _get_stored_state(self: ClickHouseClient, pipeline_name: str) -> StateInfo | None:
    self._set_query_tags(operation="get_stored_state")
    state_table = self.sql_client.make_qualified_table_name(self.schema.state_table_name)
    loads_table = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
    c_load_id, c_dlt_load_id, c_pipeline_name, c_status = self._norm_and_escape_columns(
        C_DLT_LOADS_TABLE_LOAD_ID, C_DLT_LOAD_ID, "pipeline_name", "status"
    )
    maybe_limit_clause_1, maybe_limit_clause_2 = self.sql_client._limit_clause_sql(1)

    query = (
        f"SELECT {maybe_limit_clause_1} {self.state_table_columns} FROM {state_table} AS s "
        f"WHERE s.{c_pipeline_name} = %s AND s.{c_dlt_load_id} IN ("
        f"SELECT {c_load_id} FROM {loads_table} WHERE {c_status} = 0"
        f") ORDER BY s.{c_dlt_load_id} DESC {maybe_limit_clause_2}"
    )
    with self.sql_client.execute_query(query, pipeline_name) as cur:
        row = cur.fetchone()
    if not row:
        return None
    return StateInfo(
        version=row[0],
        engine_version=row[1],
        pipeline_name=row[2],
        state=row[3],
        created_at=pendulum.instance(row[4]),
        _dlt_load_id=row[5],
    )


ClickHouseClient.get_stored_state = _get_stored_state  # type: ignore[method-assign]
