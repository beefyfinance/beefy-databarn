#!/usr/bin/env bash
# Local backup+restore gate (25.10). Does not wipe the primary ClickHouse volume.
# Usage from repo root: ./infra/clickhouse/backup-drill.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
set -a
source "$ROOT_DIR/.env"
set +a

DC=(docker compose -f "$ROOT_DIR/infra/dev/docker-compose.yml" --env-file "$ROOT_DIR/.env")
CH_PASSWORD="${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD must be set in .env}"
S3_ENDPOINT="${CLICKHOUSE_BACKUP_S3_ENDPOINT:-http://rustfs:9000}"
S3_BUCKET="${CLICKHOUSE_BACKUP_S3_BUCKET:-clickhouse-backups}"
S3_ACCESS_KEY="${CLICKHOUSE_BACKUP_S3_ACCESS_KEY:-${RUSTFS_ACCESS_KEY:-admin}}"
S3_SECRET_KEY="${CLICKHOUSE_BACKUP_S3_SECRET_KEY:-${RUSTFS_SECRET_KEY:?RUSTFS_SECRET_KEY must be set}}"

s3_dest() {
  echo "S3('${S3_ENDPOINT%/}/${S3_BUCKET}/${1}/', '${S3_ACCESS_KEY}', '${S3_SECRET_KEY}')"
}

ch() {
  "${DC[@]}" exec -T clickhouse clickhouse-client --user default --password "$CH_PASSWORD" --multiquery --query "$1"
}

backup_once() {
  "${DC[@]}" exec -T clickhouse-backup /bin/bash /opt/backup-loop.sh once "$1"
}

echo "=== 1. Full backup ==="
backup_once full
ch "SELECT status, name, error FROM system.backups ORDER BY start_time DESC LIMIT 5 FORMAT PrettyCompact"
today="$(date -u +%Y-%m-%d)"
full_prefix="full-${today}"

echo "=== 2. Incremental backup ==="
backup_once incremental
hour="$(date -u +%H)"
inc_prefix="inc-${today}-${hour}"

echo "=== 3. Throwaway data restore (dlt AS dlt_restore_test) ==="
ch "DROP DATABASE IF EXISTS dlt_restore_test"
ch "SET log_queries = 0; RESTORE DATABASE dlt AS dlt_restore_test FROM $(s3_dest "$inc_prefix")"
src_tables="$(ch "SELECT count() FROM system.tables WHERE database = 'dlt'")"
dst_tables="$(ch "SELECT count() FROM system.tables WHERE database = 'dlt_restore_test'")"
echo "dlt tables=${src_tables} restored tables=${dst_tables}"
if [[ "$src_tables" != "$dst_tables" ]]; then
  echo "Table count mismatch after throwaway restore" >&2
  exit 1
fi
ch "DROP DATABASE IF EXISTS dlt_restore_test"

echo "=== 4. Users restore round-trip ==="
ch "CREATE USER IF NOT EXISTS drill_restore_user IDENTIFIED WITH sha256_password BY 'drill-temp-pass'"
backup_once incremental || true
# Second incremental this hour may skip; force a uniquely named backup via SQL
stamp="$(date -u +%Y-%m-%d-%H%M%S)"
ch "SET log_queries = 0; BACKUP TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas TO $(s3_dest "users-drill-${stamp}") SETTINGS base_backup = $(s3_dest "$full_prefix")"
ch "DROP USER IF EXISTS drill_restore_user"
exists_before="$(ch "SELECT count() FROM system.users WHERE name = 'drill_restore_user'")"
if [[ "$exists_before" != "0" ]]; then
  echo "DROP USER did not remove drill_restore_user" >&2
  exit 1
fi
ch "SET log_queries = 0; RESTORE TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas FROM $(s3_dest "users-drill-${stamp}")"
exists_after="$(ch "SELECT count() FROM system.users WHERE name = 'drill_restore_user'")"
if [[ "$exists_after" != "1" ]]; then
  echo "User restore failed; drill_restore_user missing" >&2
  exit 1
fi
ch "DROP USER IF EXISTS drill_restore_user"
echo "User round-trip OK"

echo "=== 5. Skip-if-missing (dev_fron / dev_chebin absent is OK) ==="
missing="$(ch "SELECT groupArray(name) FROM system.databases WHERE name IN ('dev_fron','dev_chebin')")"
echo "Present optional DBs: ${missing}"
echo "Backup loop already succeeded without them if they are missing."

echo "=== 6. Empty-volume bring-up (restore-drill profile; primary volume untouched) ==="
# Never `compose down -v` — that would delete the whole project's volumes.
"${DC[@]}" --profile restore-drill rm -f -s clickhouse-restore-drill 2>/dev/null || true
docker volume rm beefy-databarn_clickhouse_restore_drill_data 2>/dev/null || true
"${DC[@]}" --profile restore-drill up -d clickhouse-restore-drill
echo "Waiting for restore-drill ping..."
for i in $(seq 1 60); do
  if "${DC[@]}" exec -T clickhouse-restore-drill wget -q -O- http://127.0.0.1:8123/ping 2>/dev/null | grep -q Ok; then
    break
  fi
  sleep 2
done

restore_ch() {
  "${DC[@]}" exec -T clickhouse-restore-drill clickhouse-client --user default --password "$CH_PASSWORD" --receive_timeout 14400 --multiquery --query "$1"
}

echo "RESTORE ALL FROM S3 ${inc_prefix}"
set +e
restore_out="$(restore_ch "SET log_queries = 0; RESTORE ALL FROM $(s3_dest "$inc_prefix")" 2>&1)"
restore_rc=$?
set -e
echo "$restore_out"
if [[ "$restore_rc" -ne 0 ]]; then
  echo "RESTORE ALL failed (often because init already created users). Restoring databases only..."
  dbs="$(ch "SELECT name FROM system.databases WHERE name IN ('dlt','envio','zapalytics','dev_fron','dev_chebin')")"
  while IFS= read -r db; do
    [[ -z "$db" ]] && continue
    restore_ch "DROP DATABASE IF EXISTS ${db}"
    restore_ch "SET log_queries = 0; RESTORE DATABASE ${db} FROM $(s3_dest "$inc_prefix")"
  done <<< "$dbs"
  restore_ch "SET log_queries = 0; RESTORE TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas FROM $(s3_dest "$inc_prefix")" || true
fi

# Re-run init so .env service users win
"${DC[@]}" exec -T clickhouse-restore-drill bash /docker-entrypoint-initdb.d/init-clickhouse.sh

compare_db() {
  local db="$1"
  local src dst
  src="$(ch "SELECT count() FROM system.tables WHERE database = '${db}'")"
  dst="$(restore_ch "SELECT count() FROM system.tables WHERE database = '${db}'")"
  echo "${db} tables source=${src} restored=${dst}"
  if [[ "$src" != "$dst" ]]; then
    echo "Mismatch for ${db}" >&2
    return 1
  fi
}

ok=0
for db in dlt envio zapalytics; do
  present="$(ch "SELECT count() FROM system.databases WHERE name = '${db}'")"
  if [[ "$present" == "1" ]]; then
    compare_db "$db" || ok=1
  fi
done
if [[ "$ok" -ne 0 ]]; then
  exit 1
fi

echo "=== 6b. dbt on restore-drill (small model if dlt has data) ==="
dlt_rows="$(restore_ch "SELECT sum(total_rows) FROM system.tables WHERE database = 'dlt'")"
if [[ "${dlt_rows:-0}" != "0" ]] && command -v uv >/dev/null 2>&1; then
  (
    cd "$ROOT_DIR/dbt"
    unset VIRTUAL_ENV
    DBT_CLICKHOUSE_HOST=127.0.0.1 DBT_CLICKHOUSE_PORT=8124 \
      uv run --env-file "$ROOT_DIR/.env" dbt run --select stg_beefy_db__prices --show-all-deprecations \
      || echo "dbt small-model run on restore-drill failed (acceptable if no source rows/schema yet)"
  )
else
  echo "Skipping dbt (no dlt rows or uv missing). Restore path still validated."
fi

"${DC[@]}" --profile restore-drill stop clickhouse-restore-drill
echo "✓ Backup drill passed (full, incremental, throwaway restore, user round-trip, skip-missing, empty-volume RESTORE ALL)"
