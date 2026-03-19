#!/bin/bash
set -euo pipefail

# Validate password: must not contain backticks to avoid unescape issues
validate_password() {
  local var_name="$1"
  local var_value="${!var_name:-}"
  
  if [[ -z "$var_value" ]]; then
    echo "Error: Environment variable $var_name is not set" >&2
    exit 1
  fi
  
  if [[ "$var_value" == *"'"* ]]; then
    echo "Error: Environment variable $var_name contains a tick (single quote), which is not allowed" >&2
    exit 1
  fi
  
  if [[ "$var_value" == *"\`"* ]]; then
    echo "Error: Environment variable $var_name contains a backtick, which is not allowed" >&2
    exit 1
  fi
}

# Validate IP config: must be either "ANY" or "IP '<cidr>'"
validate_ip_config() {
  local var_name="$1"
  local var_value="${!var_name:-}"
  
  if [[ -z "$var_value" ]]; then
    echo "Error: Environment variable $var_name is not set" >&2
    exit 1
  fi
  
  if [[ "$var_value" == "ANY" ]]; then
    return 0
  fi
  
  # Match pattern: IP '...' where ... can contain any characters except single quotes
  # Accept: IP '<cidr>' (single quotes only, without double quotes)
  if [[ "$var_value" =~ ^IP\ \'[^\'\"]+\'$ ]]; then
    return 0
  fi
  
  echo "Error: Environment variable $var_name must be either 'ANY' or 'IP '<cidr>'' (got: $var_value)" >&2
  exit 1
}

# Validate all environment variables
echo "Validating environment variables..."
validate_password CLICKHOUSE_PASSWORD

validate_password DLT_CLICKHOUSE_PASSWORD
validate_ip_config DLT_CLICKHOUSE_ALLOWED_HOST

validate_password DBT_CLICKHOUSE_PASSWORD
validate_ip_config DBT_CLICKHOUSE_ALLOWED_HOST

validate_password GRAFANA_CLICKHOUSE_PASSWORD
validate_ip_config GRAFANA_CLICKHOUSE_ALLOWED_HOST

validate_password SUPERSET_CLICKHOUSE_PASSWORD
validate_ip_config SUPERSET_CLICKHOUSE_ALLOWED_HOST

validate_password API_CLICKHOUSE_PASSWORD
validate_ip_config API_CLICKHOUSE_ALLOWED_HOST

validate_password ENVIO_CLICKHOUSE_PASSWORD
validate_ip_config ENVIO_CLICKHOUSE_ALLOWED_HOST

echo "Initializing ClickHouse databases..."

clickhouse-client \
  --user default \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery \
  --query "
    CREATE DATABASE IF NOT EXISTS analytics;
    CREATE DATABASE IF NOT EXISTS dbt;
    CREATE DATABASE IF NOT EXISTS dlt;
    CREATE DATABASE IF NOT EXISTS envio;
    CREATE DATABASE IF NOT EXISTS envio_poc1;
    CREATE DATABASE IF NOT EXISTS envio_poc2;
    CREATE DATABASE IF NOT EXISTS envio_poc3;
  "


READ_PERM="SELECT"
WRITE_PERM="INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE, CREATE DICTIONARY, DROP DICTIONARY"
RESET_DB_PERM="DROP DATABASE, CREATE DATABASE"

clickhouse-client \
  --user default \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery \
  --query "
    -------------------------
    -- Users (idempotent)
    -------------------------

    -- dlt
    CREATE USER IF NOT EXISTS dlt IDENTIFIED WITH sha256_password BY '${DLT_CLICKHOUSE_PASSWORD}';
    ALTER USER dlt IDENTIFIED WITH sha256_password BY '${DLT_CLICKHOUSE_PASSWORD}';
    ALTER USER dlt HOST ${DLT_CLICKHOUSE_ALLOWED_HOST};

    -- dbt
    CREATE USER IF NOT EXISTS dbt IDENTIFIED WITH sha256_password BY '${DBT_CLICKHOUSE_PASSWORD}';
    ALTER USER dbt IDENTIFIED WITH sha256_password BY '${DBT_CLICKHOUSE_PASSWORD}';
    ALTER USER dbt HOST ${DBT_CLICKHOUSE_ALLOWED_HOST};

    -- grafana
    CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH sha256_password BY '${GRAFANA_CLICKHOUSE_PASSWORD}';
    ALTER USER grafana IDENTIFIED WITH sha256_password BY '${GRAFANA_CLICKHOUSE_PASSWORD}';
    ALTER USER grafana HOST ${GRAFANA_CLICKHOUSE_ALLOWED_HOST};

    -- superset
    CREATE USER IF NOT EXISTS superset IDENTIFIED WITH sha256_password BY '${SUPERSET_CLICKHOUSE_PASSWORD}';
    ALTER USER superset IDENTIFIED WITH sha256_password BY '${SUPERSET_CLICKHOUSE_PASSWORD}';
    ALTER USER superset HOST ${SUPERSET_CLICKHOUSE_ALLOWED_HOST};

    -- api
    CREATE USER IF NOT EXISTS api IDENTIFIED WITH sha256_password BY '${API_CLICKHOUSE_PASSWORD}';
    ALTER USER api IDENTIFIED WITH sha256_password BY '${API_CLICKHOUSE_PASSWORD}';
    ALTER USER api HOST ${API_CLICKHOUSE_ALLOWED_HOST};

    -- envio
    CREATE USER IF NOT EXISTS envio IDENTIFIED WITH sha256_password BY '${ENVIO_CLICKHOUSE_PASSWORD}';
    ALTER USER envio IDENTIFIED WITH sha256_password BY '${ENVIO_CLICKHOUSE_PASSWORD}';
    ALTER USER envio HOST ${ENVIO_CLICKHOUSE_ALLOWED_HOST};

    -------------------------
    -- Grants (idempotent)
    -------------------------

    -- dlt: RW on dlt.*
    REVOKE ALL PRIVILEGES ON INFORMATION_SCHEMA.*      FROM dlt;
    REVOKE ALL PRIVILEGES ON dlt.*                     FROM dlt;
    REVOKE ALL PRIVILEGES ON dbt.*                     FROM dlt;
    REVOKE ALL PRIVILEGES ON analytics.*               FROM dlt;
    GRANT ${READ_PERM}                ON INFORMATION_SCHEMA.*       TO dlt;
    GRANT ${READ_PERM}, ${WRITE_PERM} ON dlt.*                      TO dlt;
    GRANT ${READ_PERM}                ON dbt.*                      TO dlt; -- required to update incremental materialized views
    GRANT ${READ_PERM}                ON analytics.*                TO dlt; -- required to update incremental materialized views

    -- dbt: R on dlt.*, RW on dbt.* & analytics.*
    REVOKE ALL PRIVILEGES ON INFORMATION_SCHEMA.*     FROM dbt;
    REVOKE ALL PRIVILEGES ON dlt.*                    FROM dbt;
    REVOKE ALL PRIVILEGES ON envio.*                  FROM dbt;
    REVOKE ALL PRIVILEGES ON envio_poc1.*             FROM dbt;
    REVOKE ALL PRIVILEGES ON envio_poc2.*             FROM dbt;
    REVOKE ALL PRIVILEGES ON envio_poc3.*             FROM dbt;
    REVOKE ALL PRIVILEGES ON dbt.*                    FROM dbt;
    REVOKE ALL PRIVILEGES ON analytics.*              FROM dbt;
    GRANT ${READ_PERM}                ON INFORMATION_SCHEMA.*       TO dbt;
    GRANT ${READ_PERM}                ON dlt.*                      TO dbt;
    GRANT ${READ_PERM}                ON envio.*                    TO dbt;
    GRANT ${READ_PERM}                ON envio_poc1.*               TO dbt;
    GRANT ${READ_PERM}                ON envio_poc2.*               TO dbt;
    GRANT ${READ_PERM}                ON envio_poc3.*               TO dbt;
    GRANT ${READ_PERM}, ${WRITE_PERM} ON dbt.*                      TO dbt;
    GRANT ${READ_PERM}, ${WRITE_PERM} ON analytics.*                TO dbt;

    -- grafana: R on dlt.*, dbt.*, analytics.*
    REVOKE ALL PRIVILEGES ON dlt.*           FROM grafana;
    REVOKE ALL PRIVILEGES ON dbt.*           FROM grafana;
    REVOKE ALL PRIVILEGES ON analytics.*     FROM grafana;
    REVOKE ALL PRIVILEGES ON envio.*         FROM grafana;
    REVOKE ALL PRIVILEGES ON envio_poc1.*    FROM grafana;
    REVOKE ALL PRIVILEGES ON envio_poc2.*    FROM grafana;
    REVOKE ALL PRIVILEGES ON envio_poc3.*    FROM grafana;
    GRANT ${READ_PERM} ON dlt.*           TO grafana;
    GRANT ${READ_PERM} ON dbt.*           TO grafana;
    GRANT ${READ_PERM} ON analytics.*     TO grafana;
    GRANT ${READ_PERM} ON envio.*         TO grafana;
    GRANT ${READ_PERM} ON envio_poc1.*    TO grafana;
    GRANT ${READ_PERM} ON envio_poc2.*    TO grafana;
    GRANT ${READ_PERM} ON envio_poc3.*    TO grafana;

    -- superset: R on analytics.*
    REVOKE ALL PRIVILEGES ON dlt.*           FROM superset;
    REVOKE ALL PRIVILEGES ON dbt.*           FROM superset;
    REVOKE ALL PRIVILEGES ON analytics.*     FROM superset;
    REVOKE ALL PRIVILEGES ON envio.*         FROM superset;
    REVOKE ALL PRIVILEGES ON envio_poc1.*    FROM superset;
    REVOKE ALL PRIVILEGES ON envio_poc2.*    FROM superset;
    REVOKE ALL PRIVILEGES ON envio_poc3.*    FROM superset;
    GRANT ${READ_PERM} ON dlt.*        TO superset;
    GRANT ${READ_PERM} ON dbt.*        TO superset;
    GRANT ${READ_PERM} ON analytics.*  TO superset;
    GRANT ${READ_PERM} ON envio.*      TO superset;
    GRANT ${READ_PERM} ON envio_poc1.* TO superset;
    GRANT ${READ_PERM} ON envio_poc2.* TO superset;
    GRANT ${READ_PERM} ON envio_poc3.* TO superset;

    -- api: R on analytics.*
    REVOKE ALL PRIVILEGES ON analytics.*    FROM api;
    REVOKE ALL PRIVILEGES ON dlt.*          FROM api;
    REVOKE ALL PRIVILEGES ON dbt.*          FROM api;
    REVOKE ALL PRIVILEGES ON envio.*        FROM api;
    REVOKE ALL PRIVILEGES ON envio_poc1.*   FROM api;
    REVOKE ALL PRIVILEGES ON envio_poc2.*   FROM api;
    REVOKE ALL PRIVILEGES ON envio_poc3.*   FROM api;
    GRANT ${READ_PERM} ON analytics.*    TO api;
    GRANT ${READ_PERM} ON dlt.*          TO api;
    GRANT ${READ_PERM} ON dbt.*          TO api;
    GRANT ${READ_PERM} ON envio.*        TO api;
    GRANT ${READ_PERM} ON envio_poc1.*   TO api;
    GRANT ${READ_PERM} ON envio_poc2.*   TO api;
    GRANT ${READ_PERM} ON envio_poc3.*   TO api;

    -- envio-sync: R on analytics.*
    REVOKE ALL PRIVILEGES ON envio.*        FROM envio;
    REVOKE ALL PRIVILEGES ON envio_poc1.*   FROM envio;
    REVOKE ALL PRIVILEGES ON envio_poc2.*   FROM envio;
    REVOKE ALL PRIVILEGES ON envio_poc3.*   FROM envio;
    GRANT ${READ_PERM}, ${WRITE_PERM}, ${RESET_DB_PERM} ON envio.*        TO envio;
    GRANT ${READ_PERM}, ${WRITE_PERM}, ${RESET_DB_PERM} ON envio_poc1.*   TO envio;
    GRANT ${READ_PERM}, ${WRITE_PERM}, ${RESET_DB_PERM} ON envio_poc2.*   TO envio;
    GRANT ${READ_PERM}, ${WRITE_PERM}, ${RESET_DB_PERM} ON envio_poc3.*   TO envio;

    -------------------------------------------
    -- Settings profiles (env-synced)
    -------------------------------------------

    -- Web profile: readonly dashboards (grafana + superset + api)
    CREATE SETTINGS PROFILE IF NOT EXISTS web_profile;
    ALTER  SETTINGS PROFILE web_profile
        SETTINGS
            readonly = 1,
            max_execution_time = ${CLICKHOUSE_WEB_MAX_EXECUTION_TIME:-180}
                MIN 0
                MAX 180
                CHANGEABLE_IN_READONLY,
            max_memory_usage = ${CLICKHOUSE_WEB_MAX_MEMORY_USAGE:-6000000000}
                MIN 0
                MAX 10000000000
                CHANGEABLE_IN_READONLY,
            max_result_rows   = ${CLICKHOUSE_WEB_MAX_RESULT_ROWS:-100000},
            max_rows_to_read  = ${CLICKHOUSE_WEB_MAX_ROWS_TO_READ:-1000000},
            use_uncompressed_cache = 0,
            load_balancing = 'random'
        TO grafana, superset, api;

    -- ETL profile: dbt + dlt
    CREATE SETTINGS PROFILE IF NOT EXISTS etl_profile;
    ALTER  SETTINGS PROFILE etl_profile
        SETTINGS
            max_execution_time = 3600,
            max_memory_usage   = ${CLICKHOUSE_MAX_MEMORY_USAGE:-10000000000}
        TO dlt, dbt;

    -- External profile
    CREATE SETTINGS PROFILE IF NOT EXISTS external_profile;
    ALTER  SETTINGS PROFILE external_profile
        SETTINGS
            max_execution_time = ${CLICKHOUSE_EXTERNAL_MAX_MEMORY_USAGE:-20},
            max_memory_usage   = ${CLICKHOUSE_EXTERNAL_MAX_MEMORY_USAGE:-10000000000},
            max_result_rows    = ${CLICKHOUSE_EXTERNAL_MAX_RESULT_ROWS:-100000},
            max_rows_to_read   = ${CLICKHOUSE_EXTERNAL_MAX_ROWS_TO_READ:-1000000},
            use_uncompressed_cache = 0,
            load_balancing = 'random'
        TO envio;


    -------------------------------------------
    -- Quotas (same as your XML quotas)
    -------------------------------------------

    -- Optional default quota (effectively unlimited, like XML <default>)
    CREATE QUOTA OR REPLACE default_quota
        FOR INTERVAL 3600 SECOND MAX
            queries        = 0,
            query_selects  = 0,
            errors         = 0,
            result_rows    = 0,
            result_bytes   = 0,
            read_rows      = 0,
            read_bytes     = 0,
            execution_time = 0
        TO dlt, dbt;

    -- Web quota: limit dashboard workloads
    CREATE QUOTA OR REPLACE web_quota
        FOR INTERVAL 3600 SECOND MAX
            queries        = 5000,
            query_selects  = 5000,
            errors         = 1000,
            result_rows    = 10000000000,
            result_bytes   = 10000000000000,
            read_rows      = 100000000000,
            read_bytes     = 100000000000000,
            execution_time = 7200
        TO grafana, superset, api;

    -- External quota: limit external workloads
    CREATE QUOTA OR REPLACE external_quota
        FOR INTERVAL 1 SECOND MAX
            queries        = 5000,
            query_selects  = 5000,
            errors         = 1000,
            result_rows    = 10000000000,
            result_bytes   = 10000000000000,
            read_rows      = 100000000000,
            read_bytes     = 100000000000000,
            execution_time = 7200
        TO envio;
"

echo "✓ Databases analytics, dbt & dlt initialized"
echo "✓ Users, grants, profiles, quotas reset & synced to env"
