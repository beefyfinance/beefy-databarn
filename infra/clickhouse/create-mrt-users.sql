-- mrt dev accounts. Run as the ClickHouse default admin:
--   docker exec -it beefy-databarn-clickhouse-1 clickhouse-client --multiquery < create-mrt-users.sql
-- Replace both <password> placeholders first (no single quotes or backticks, see init-clickhouse.sh).

-------------------------------------------
-- envio_dev_mrt: read write on every envio_dev_mrt_* database
-------------------------------------------
CREATE USER IF NOT EXISTS envio_dev_mrt
IDENTIFIED WITH sha256_password BY <password>
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

CREATE USER IF NOT EXISTS beefy_indexer_worker_dev_mrt
IDENTIFIED WITH sha256_password BY <password>
HOST ANY
SETTINGS PROFILE 'envio_profile';

GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE
    ON beefy_indexer_dev_mrt.* TO beefy_indexer_worker_dev_mrt;

CREATE USER IF NOT EXISTS beefy_indexer_api_dev_mrt
IDENTIFIED WITH sha256_password BY <password>
HOST ANY
SETTINGS PROFILE 'web_profile';

GRANT SELECT ON beefy_indexer_dev_mrt.* TO beefy_indexer_api_dev_mrt;
