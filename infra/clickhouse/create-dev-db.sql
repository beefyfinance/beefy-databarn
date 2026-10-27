-- Developer sandbox database. Replace <user> and run as the ClickHouse default admin.
-- Same project write shape as zapalytics/envio (no DROP DATABASE).

CREATE DATABASE IF NOT EXISTS dev_<user>;
GRANT SELECT, INSERT, CREATE TABLE, ALTER, DROP TABLE, TRUNCATE, OPTIMIZE
    ON dev_<user>.* TO <user>;

GRANT SELECT ON dev_<user>.* TO superset;
GRANT SELECT ON dev_<user>.* TO api;
GRANT SELECT ON dev_<user>.* TO grafana;
GRANT SELECT ON dev_<user>.* TO mrt;
GRANT SELECT ON system.completions TO <user>;

-- Daily write quota (dev_quota) comes from the role, declared in init-clickhouse.sh.
GRANT dev_role TO <user>;
