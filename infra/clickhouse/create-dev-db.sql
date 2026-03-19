
CREATE DATABASE IF NOT EXISTS dev_<user>;
GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE
    ON dev_<user>.* TO <user>;

GRANT SELECT ON dev_<user>.* TO superset;
GRANT SELECT ON dev_<user>.* TO api;
GRANT SELECT ON dev_<user>.* TO grafana;
GRANT SELECT ON dev_<user>.* TO mrt;
GRANT SELECT ON system.completions TO <user>;

CREATE QUOTA OR REPLACE dev_<user>_write_quota
    FOR INTERVAL 1 DAY MAX
        written_bytes = 1073741824
    TO <user>;
