-- docker exec -it beefy-databarn-clickhouse-1 clickhouse-client

CREATE DATABASE IF NOT EXISTS <database>;

CREATE USER IF NOT EXISTS <username>
IDENTIFIED WITH sha256_password BY <password>
HOST ANY
SETTINGS PROFILE 'external_profile';

-- read write
GRANT SELECT, INSERT, ALTER, CREATE TABLE ON <database>.* TO <username>;

-- read only
GRANT SELECT ON <database>.* TO <username>;

ALTER QUOTA external_quota TO <username>;
