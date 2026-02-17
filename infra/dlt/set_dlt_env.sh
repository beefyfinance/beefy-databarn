#!/usr/bin/env bash
# Map databarn .env vars to DLT env var names (DLT uses SECTION__KEY with double underscore, uppercase).
# Source before running pipelines: from repo root: . ./infra/dlt/set_dlt_env.sh  or from dlt/: . ../infra/dlt/set_dlt_env.sh
# See https://dlthub.com/docs and dlt/common/configuration/providers/environ.py

set -e
if [ -f .env ]; then set -a; source .env; set +a
elif [ -f ../.env ]; then set -a; source ../.env; set +a
elif [ -f ../../.env ]; then set -a; source ../../.env; set +a; fi

# --- Destination: filesystem ---
# In prod use RustFS (S3-compatible); otherwise use local file storage (file://)
if [ "${DLT_ENV:-}" = "production" ]; then
  # RustFS / S3-compatible staging
  rustfs_bucket="${RUSTFS_DLT_STAGING_BUCKET:?RUSTFS_DLT_STAGING_BUCKET must be set for production}"
  rustfs_access="${RUSTFS_ACCESS_KEY:?RUSTFS_ACCESS_KEY must be set for production}"
  rustfs_secret="${RUSTFS_SECRET_KEY:?RUSTFS_SECRET_KEY must be set for production}"
  rustfs_endpoint="${RUSTFS_ENDPOINT:?RUSTFS_ENDPOINT must be set for production}"
  export DESTINATION__FILESYSTEM__BUCKET_URL="s3://${rustfs_bucket}"
  export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="${rustfs_access}"
  export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="${rustfs_secret}"
  export DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL="${rustfs_endpoint}"
else
  # Local file storage
  storage_dir="${STORAGE_DIR:?STORAGE_DIR must be set for non-production environments}"
  dst_dir="${storage_dir}/dlt"
  mkdir -p "${dst_dir}"
  export DESTINATION__FILESYSTEM__BUCKET_URL="file://${dst_dir}"
  # Unset S3 credentials so dlt uses local filesystem only
  unset DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID
  unset DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY
  unset DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL
fi

# --- Destination: ClickHouse ---
# In prod these come from docker-compose (env_file: .env). Ensure .env has DLT_CLICKHOUSE_* set.
: "${DLT_CLICKHOUSE_HOST:?DLT_CLICKHOUSE_HOST must be set (in .env at repo root for prod).}"
: "${DLT_CLICKHOUSE_USER:?DLT_CLICKHOUSE_USER must be set}"
: "${DLT_CLICKHOUSE_PASSWORD:?DLT_CLICKHOUSE_PASSWORD must be set (e.g. in .env at repo root for prod).}"
: "${DLT_CLICKHOUSE_DB:?DLT_CLICKHOUSE_DB must be set}"
export DESTINATION__CLICKHOUSE__CREDENTIALS__HOST="${DLT_CLICKHOUSE_HOST}"
export DESTINATION__CLICKHOUSE__CREDENTIALS__PORT="${DLT_CLICKHOUSE_PORT:-9000}"
export DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT="${DLT_CLICKHOUSE_HTTP_PORT:-8123}"
export DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME="${DLT_CLICKHOUSE_USER}"
export DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD="${DLT_CLICKHOUSE_PASSWORD}"
export DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE="${DLT_CLICKHOUSE_DB}"
export DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE="${DLT_CLICKHOUSE_SECURE:-0}"

# --- Source: beefy_db (PostgreSQL) ---
# Single connection string; DLT reads SOURCES__<source_name>__CREDENTIALS
: "${BEEFY_DB_USER:?BEEFY_DB_USER must be set}"
: "${BEEFY_DB_PASSWORD:?BEEFY_DB_PASSWORD must be set}"
: "${BEEFY_DB_HOST:?BEEFY_DB_HOST must be set}"
: "${BEEFY_DB_PORT:?BEEFY_DB_PORT must be set}"
: "${BEEFY_DB_NAME:?BEEFY_DB_NAME must be set}"
export SOURCES__BEEFY_DB__CREDENTIALS="postgresql://${BEEFY_DB_USER}:${BEEFY_DB_PASSWORD}@${BEEFY_DB_HOST}:${BEEFY_DB_PORT}/${BEEFY_DB_NAME}?sslmode=${BEEFY_DB_SSLMODE:-require}"

# --- Runtime / load / extract / normalize (optional; defaults can come from .dlt/config.toml) ---
export LOAD__TRUNCATE_STAGING_DATASET="${LOAD__TRUNCATE_STAGING_DATASET:-true}"
export RUNTIME__LOG_LEVEL="${RUNTIME__LOG_LEVEL:-INFO}"
export EXTRACT__WORKERS="${EXTRACT__WORKERS:-3}"
export EXTRACT__DATA_WRITER__DISABLE_COMPRESSION="${EXTRACT__DATA_WRITER__DISABLE_COMPRESSION:-true}"
export EXTRACT__DATA_WRITER__BUFFER_MAX_ITEMS="${EXTRACT__DATA_WRITER__BUFFER_MAX_ITEMS:-1000000}"
export EXTRACT__DATA_WRITER__FILE_MAX_ITEMS="${EXTRACT__DATA_WRITER__FILE_MAX_ITEMS:-1000000}"
export NORMALIZE__WORKERS="${NORMALIZE__WORKERS:-3}"
export NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION="${NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION:-true}"
export NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS="${NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS:-1000000}"
export NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS="${NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS:-1000000}"
export LOAD__WORKERS="${LOAD__WORKERS:-3}"
export LOAD__DATA_WRITER__DISABLE_COMPRESSION="${LOAD__DATA_WRITER__DISABLE_COMPRESSION:-true}"
export LOAD__DATA_WRITER__BUFFER_MAX_ITEMS="${LOAD__DATA_WRITER__BUFFER_MAX_ITEMS:-1000000}"
export LOAD__DATA_WRITER__FILE_MAX_ITEMS="${LOAD__DATA_WRITER__FILE_MAX_ITEMS:-1000000}"
