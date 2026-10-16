"""Do not use Postgres named cursors for Timescale extracts.

dlt's pyarrow loader sets ``yield_per=chunk_size``, which psycopg2 turns
into DECLARE CURSOR + FETCH FORWARD. That is the only reason a transaction
stayed open: Postgres refuses DECLARE CURSOR outside a transaction block,
so extract sat idle-in-transaction while Python converted Arrow batches.

We never needed to write, commit, or autocommit. Run one SELECT, buffer the
rows, return the connection (SQLAlchemy rolls back the unused read
snapshot), then convert locally. Timescale is done before Arrow work.
"""

from typing import Any, Dict, Iterator, Sequence

from dlt.sources.sql_database.helpers import TableLoader


class _BufferedResult:
    def __init__(self, columns: Sequence[str], partitions: list[list[Any]]) -> None:
        self._columns = columns
        self._partitions = partitions

    def keys(self) -> Sequence[str]:
        return self._columns

    def partitions(self, size: int | None = None) -> Iterator[list[Any]]:
        yield from self._partitions

    def close(self) -> None:
        return None


def _load_rows_without_server_cursor(
    self: TableLoader, query: Any, backend_kwargs: Dict[str, Any]
) -> Iterator[Any]:
    with self.engine.connect() as conn:
        result = conn.execute(query)
        try:
            buffered = _BufferedResult(
                list(result.keys()),
                [list(p) for p in result.partitions(size=self.chunk_size)],
            )
        finally:
            result.close()
    yield from self._convert_result(buffered, backend_kwargs)


TableLoader._load_rows = _load_rows_without_server_cursor  # type: ignore[method-assign]
