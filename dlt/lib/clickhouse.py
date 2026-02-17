import clickhouse_connect
from lib.config import get_clickhouse_credentials

# Cache for the ClickHouse async client
_client_cache: clickhouse_connect.driver.asyncclient.AsyncClient | None = None

async def get_clickhouse_client() -> clickhouse_connect.driver.asyncclient.AsyncClient:
    """Create and return a cached ClickHouse async client from dlt credentials."""
    global _client_cache
    
    if _client_cache is None:
        credentials = get_clickhouse_credentials()
        _client_cache = await clickhouse_connect.get_async_client(
            host=credentials["host"],
            port=credentials["http_port"],
            user=credentials["user"],
            password=credentials["password"],
            database=credentials["database"],
            secure=credentials["secure"],
        )
    
    return _client_cache


def clickhouse_default_database() -> str:
    """Get the default database from the ClickHouse credentials."""
    return get_clickhouse_credentials()["database"]

async def clickhouse_table_exists(table_name: str, database: str | None = None) -> bool:
    """
    Test if a table exists in ClickHouse.

    Args:
        table_name: Name of the table to check. Can be qualified with database (e.g. "mydb.mytable").
        database: If provided, will check in this database (overrides default in credentials).

    Returns:
        True if the table exists, False otherwise.
    """
    client = await get_clickhouse_client()
    # Determine where clause for database and table parsing
    if "." in table_name:
        db, tbl = table_name.split(".", 1)
    else:
        db = database or clickhouse_default_database()
        tbl = table_name

    query = """
        SELECT count() 
        FROM system.tables 
        WHERE database = %(database)s AND name = %(table)s
    """
    result = await client.query(query, parameters={"database": db, "table": tbl})
    count = result.result_rows[0][0] if result.result_rows else 0
    return count > 0
