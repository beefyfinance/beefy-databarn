-- mrt dev accounts. Run as the ClickHouse default admin:
--   docker exec -it beefy-databarn-clickhouse-1 clickhouse-client --multiquery < create-mrt-users.sql
-- Replace the three <password> placeholders first (no single quotes or backticks, see init-clickhouse.sh).

-------------------------------------------
-- envio_dev_mrt: read write on every envio_dev_mrt_* database
-------------------------------------------
CREATE USER IF NOT EXISTS envio_dev_mrt
IDENTIFIED WITH sha256_password BY '<password>'
HOST ANY
SETTINGS PROFILE 'envio_profile';

-- Wildcard grants also cover databases created later. CREATE/DROP DATABASE is
-- included because the envio indexer creates and resets its own databases.
GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE,
      CREATE DATABASE, DROP DATABASE
    ON envio_dev_mrt_*.* TO envio_dev_mrt;

-------------------------------------------
-- beefy_indexer_dev_mrt database: split into a worker (rw) and an api (ro) account
-------------------------------------------
CREATE DATABASE IF NOT EXISTS beefy_indexer_dev_mrt;

-- Replaced by the two accounts below. The database and its data are kept.
DROP USER IF EXISTS beefy_indexer_dev_mrt;

-- envio_profile caps max_rows_to_read at 1M and max_execution_time at 120s, too low for
-- the stamping views and backfills. Own profile, not tied to init-clickhouse.sh.
CREATE SETTINGS PROFILE IF NOT EXISTS indexer_worker_dev_profile;
ALTER SETTINGS PROFILE indexer_worker_dev_profile
    SETTINGS
        max_rows_to_read = 0,
        max_execution_time = 3600,
        max_memory_usage = 10000000000,
        enable_lightweight_delete = 0 MIN 0 MAX 0,
        use_uncompressed_cache = 0,
        load_balancing = 'random';

CREATE USER IF NOT EXISTS beefy_indexer_worker_dev_mrt
IDENTIFIED WITH sha256_password BY '<password>'
HOST ANY
SETTINGS PROFILE 'indexer_worker_dev_profile';
-- Existing account: CREATE ... IF NOT EXISTS does not change its profile.
ALTER USER beefy_indexer_worker_dev_mrt SETTINGS PROFILE 'indexer_worker_dev_profile';

GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE,
      CREATE VIEW, DROP VIEW, CREATE DATABASE
    ON beefy_indexer_dev_mrt.* TO beefy_indexer_worker_dev_mrt;

-- The worker reads the warehouse and the dev envio databases.
GRANT SELECT ON dbt.*              TO beefy_indexer_worker_dev_mrt;
GRANT SELECT ON analytics.*        TO beefy_indexer_worker_dev_mrt;
GRANT SELECT ON dlt.*              TO beefy_indexer_worker_dev_mrt;
GRANT SELECT ON envio_dev_mrt_*.*  TO beefy_indexer_worker_dev_mrt;

-- Views created WITH DEFINER = beefy_indexer_worker_dev_mrt.
GRANT SET DEFINER ON beefy_indexer_worker_dev_mrt TO beefy_indexer_worker_dev_mrt;

CREATE USER IF NOT EXISTS beefy_indexer_api_dev_mrt
IDENTIFIED WITH sha256_password BY '<password>'
HOST ANY
SETTINGS PROFILE 'web_profile';

GRANT SELECT ON beefy_indexer_dev_mrt.* TO beefy_indexer_api_dev_mrt;
