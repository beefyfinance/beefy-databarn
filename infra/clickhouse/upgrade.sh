#!/usr/bin/env bash
# Prod 25.10 → 26.8. Never deletes data dirs.
#
#   cd infra/prod
#   make clickhouse migrate CONFIRM=1
#   make clickhouse rollback CONFIRM=1
set -euo pipefail

if [[ "$(whoami)" != "databarn" ]]; then
  echo "Run on the prod host: cd infra/prod && make clickhouse migrate CONFIRM=1" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
set -a
source "$ROOT_DIR/.env"
set +a

DC=(docker compose -f "$ROOT_DIR/infra/prod/docker-compose.yml" --env-file "$ROOT_DIR/.env")
STORAGE_DIR="${STORAGE_DIR:-/mnt/data}"
LIVE_DIR="${STORAGE_DIR}/clickhouse"
NEW_DIR="${STORAGE_DIR}/clickhouse-26.8"
STASH_DIR="${STORAGE_DIR}/clickhouse.bak"
FAILED_DIR="${STORAGE_DIR}/clickhouse.failed"
PREFIX_FILE="${STORAGE_DIR}/clickhouse-migrate.prefix"
CH_VOLUME="beefy-databarn_clickhouse_data"
NEED_GB="${CLICKHOUSE_MIGRATE_FREE_GB:-120}"
TARGET_VERSION="${CLICKHOUSE_MIGRATE_VERSION:-26.8.4}"
ROLLBACK_VERSION="${CLICKHOUSE_ROLLBACK_VERSION:-25.10}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD must be set in .env}"
BACKUP_DATABASES=(dlt envio zapalytics dev_fron dev_chebin)

need_confirm() {
  if [[ "${CONFIRM:-}" != "1" ]]; then
    echo "Refusing. Re-run with CONFIRM=1" >&2
    exit 1
  fi
}

# Move aside; never delete.
aside() {
  local src="$1" dest n=0
  [[ -e "$src" ]] || return 0
  dest="${src}.$(date -u +%Y%m%d-%H%M%S)"
  while [[ -e "$dest" ]]; do
    n=$((n + 1))
    dest="${src}.$(date -u +%Y%m%d-%H%M%S).${n}"
  done
  echo "Moving ${src} → ${dest} (kept)"
  mv "$src" "$dest"
}

pin_clickhouse_version() {
  local ver="$1" envf="$ROOT_DIR/.env" tmp
  tmp="$(mktemp)"
  awk -v ver="$ver" '
    BEGIN { done=0 }
    /^CLICKHOUSE_VERSION=/ && !done { print "CLICKHOUSE_VERSION=" ver; done=1; next }
    { print }
    END { if (!done) print "CLICKHOUSE_VERSION=" ver }
  ' "$envf" > "$tmp" && mv "$tmp" "$envf"
  echo "Pinned CLICKHOUSE_VERSION=${ver} in .env"
}

df_ok() {
  local avail_kb avail_gb
  mkdir -p "$STORAGE_DIR"
  avail_kb="$(df -k "$STORAGE_DIR" | awk 'NR==2 {print $4}')"
  avail_gb=$((avail_kb / 1024 / 1024))
  echo "Free on ${STORAGE_DIR}: ${avail_gb}G (need ${NEED_GB}G while 25.10 is kept)"
  if [[ "${avail_gb:-0}" -lt "$NEED_GB" ]]; then
    echo "Not enough disk. Free space or set CLICKHOUSE_MIGRATE_FREE_GB." >&2
    exit 1
  fi
}

require_avx2() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    echo "avx2: skipped (not Linux)"
    return 0
  fi
  if grep -q avx2 /proc/cpuinfo; then
    echo "avx2: yes"
    return 0
  fi
  echo "avx2: NO — ${TARGET_VERSION} will not start" >&2
  exit 1
}

stop_writers() {
  "${DC[@]}" stop dlt dbt || true
}

start_writers() {
  "${DC[@]}" start dlt dbt || "${DC[@]}" up -d dlt dbt
}

release_clickhouse_bind() {
  "${DC[@]}" rm -f clickhouse clickhouse-backup 2>/dev/null || true
  if docker volume inspect "$CH_VOLUME" >/dev/null 2>&1; then
    echo "Releasing bind volume ${CH_VOLUME} (host files kept)"
    docker volume rm "$CH_VOLUME"
  fi
}

wait_ch() {
  local i
  for i in $(seq 1 90); do
    if "${DC[@]}" exec -T clickhouse wget -O - --no-verbose --tries=1 http://127.0.0.1:8123/ping 2>/dev/null | grep -q Ok; then
      break
    fi
    if [[ "$i" -eq 90 ]]; then
      echo "ClickHouse did not become ready" >&2
      exit 1
    fi
    sleep 2
  done
  for i in $(seq 1 60); do
    if "${DC[@]}" exec -T clickhouse clickhouse-client --user default --password "$CH_PASSWORD" --query "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "ClickHouse HTTP is up but the native client is not" >&2
  exit 1
}

pick_backup_prefix() {
  local today hour status
  today="$(date -u +%Y-%m-%d)"
  hour="$(date -u +%H)"
  status="$("${DC[@]}" exec -T clickhouse-backup /bin/bash /opt/backup-loop.sh status)"
  echo "$status" >&2
  if echo "$status" | grep -qE "^inc-${today}-${hour}/?$"; then
    echo "inc-${today}-${hour}"
    return
  fi
  if echo "$status" | grep -qE "^full-${today}/?$"; then
    echo "full-${today}"
    return
  fi
  echo "$status" | awk '/^(full|inc)-/ {gsub(/\/$/,""); print}' | sort | tail -1
}

running_version() {
  "${DC[@]}" exec -T clickhouse clickhouse-client --query "SELECT version()" 2>/dev/null || true
}

s3_dest() {
  local ep="${CLICKHOUSE_BACKUP_S3_ENDPOINT:-http://rustfs:9000}"
  local bucket="${CLICKHOUSE_BACKUP_S3_BUCKET:-clickhouse-backups}"
  local key="${CLICKHOUSE_BACKUP_S3_ACCESS_KEY:-${RUSTFS_ACCESS_KEY:-admin}}"
  local secret="${CLICKHOUSE_BACKUP_S3_SECRET_KEY:-${RUSTFS_SECRET_KEY:?S3 secret must be set}}"
  echo "S3('${ep%/}/${bucket}/${1}/', '${key}', '${secret}')"
}

chq() {
  "${DC[@]}" exec -T clickhouse clickhouse-client --user default --password "$CH_PASSWORD" \
    --receive_timeout 14400 --send_timeout 14400 --multiquery --query "$1"
}

table_counts() {
  chq "SELECT database, count() AS tables FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') GROUP BY database ORDER BY database FORMAT PrettyCompact"
}

restored_table_count() {
  local n
  n="$(chq "SELECT count() FROM system.tables WHERE database IN ('dlt','envio','zapalytics','dev_fron','dev_chebin')" 2>/dev/null | tr -cd '0-9')"
  echo "${n:-0}"
}

stop_ch() {
  "${DC[@]}" stop clickhouse-backup clickhouse 2>/dev/null || true
  release_clickhouse_bind
}

start_clickhouse() {
  local dir_name="$1" ver="$2"
  export CLICKHOUSE_DIR="$dir_name"
  export CLICKHOUSE_VERSION="$ver"
  # Pin .env only when the live path is in use, so a failed sibling restore
  # cannot make `make infra start` boot 26.8 onto 25.10 data.
  if [[ "$dir_name" == "clickhouse" ]]; then
    pin_clickhouse_version "$ver"
  fi
  echo "Starting ClickHouse ${ver} on ${STORAGE_DIR}/${dir_name}..."
  "${DC[@]}" up -d --wait --wait-timeout 300 --force-recreate --build --no-deps clickhouse
  wait_ch
}

start_backup() {
  "${DC[@]}" up -d --wait --wait-timeout 180 --force-recreate --build --no-deps clickhouse-backup
}

show_status() {
  local ver
  echo "=== migrate dirs on ${STORAGE_DIR} ==="
  for p in "$LIVE_DIR" "$NEW_DIR" "$STASH_DIR" "$FAILED_DIR"; do
    if [[ -e "$p" ]]; then
      echo "  present  $p"
    else
      echo "  absent   $p"
    fi
  done
  if [[ -f "$PREFIX_FILE" ]]; then
    echo "  prefix   $(cat "$PREFIX_FILE")"
  fi
  ver="$(running_version)"
  if [[ -n "$ver" ]]; then
    echo "=== running version ==="
    echo "  $ver"
  else
    echo "=== running version ==="
    echo "  (clickhouse not reachable)"
  fi
}

restore_backup() {
  local prefix="$1" db restored=0 n
  n="$(restored_table_count)"
  n="${n:-0}"
  if [[ "$n" -gt 0 ]]; then
    echo "Warehouse tables already present (${n}); skipping RESTORE"
    return 0
  fi
  echo "RESTORE ALL ${prefix}"
  set +e
  chq "SET log_queries = 0; RESTORE ALL FROM $(s3_dest "$prefix")"
  local rc=$?
  set -e
  if [[ "$rc" -eq 0 ]]; then
    return 0
  fi
  echo "RESTORE ALL failed (often because init already created users). Restoring databases..."
  for db in "${BACKUP_DATABASES[@]}"; do
    n="$(chq "SELECT count() FROM system.tables WHERE database = '${db}'" 2>/dev/null || echo 0)"
    if [[ "${n:-0}" != "0" ]]; then
      echo "Keeping ${db} (${n} tables already there)"
      restored=$((restored + 1))
      continue
    fi
    chq "DROP DATABASE IF EXISTS ${db}" || true
    if chq "SET log_queries = 0; RESTORE DATABASE ${db} FROM $(s3_dest "$prefix")"; then
      restored=$((restored + 1))
    fi
  done
  chq "SET log_queries = 0; RESTORE TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas FROM $(s3_dest "$prefix")" || true
  if [[ "$restored" -eq 0 ]]; then
    echo "Restore failed. 25.10 is still at ${LIVE_DIR}. Rollback: make clickhouse rollback CONFIRM=1" >&2
    exit 1
  fi
}

finish_cutover() {
  echo "Pointing live path at 26.8..."
  stop_ch
  if [[ ! -d "$STASH_DIR" ]]; then
    if [[ ! -d "$LIVE_DIR" ]]; then
      echo "Neither ${LIVE_DIR} nor ${STASH_DIR} exists; cannot cut over." >&2
      exit 1
    fi
    mv "$LIVE_DIR" "$STASH_DIR"
  fi
  if [[ -d "$NEW_DIR" ]]; then
    if [[ -d "$LIVE_DIR" ]]; then
      echo "${LIVE_DIR} and ${NEW_DIR} both exist. Move one aside, then re-run migrate." >&2
      exit 1
    fi
    mv "$NEW_DIR" "$LIVE_DIR"
  fi
  if [[ ! -d "$LIVE_DIR" ]]; then
    echo "No live dir after cutover." >&2
    exit 1
  fi
  start_clickhouse clickhouse "$TARGET_VERSION"
  start_backup
  start_writers
  echo "=== table counts ==="
  table_counts
  echo "✓ Migrated to ${TARGET_VERSION}."
  echo "  25.10 data: ${STASH_DIR}  (keep until soak is done)"
  echo "  Rollback:   make clickhouse rollback CONFIRM=1"
}

cmd="${1:-help}"
case "$cmd" in
  status)
    show_status
    ;;
  migrate)
    need_confirm
    df_ok
    require_avx2
    show_status

    ver="$(running_version)"
    if [[ "$ver" == 26.8* && -d "$STASH_DIR" && ! -d "$NEW_DIR" ]]; then
      echo "Already on ${ver}. Nothing to do."
      table_counts
      exit 0
    fi

    # Cutover dirs already swapped except start, or rename still pending.
    if [[ -d "$STASH_DIR" && ! -d "$NEW_DIR" && -d "$LIVE_DIR" && "$ver" == 26.8* ]]; then
      echo "Already on ${ver}."
      exit 0
    fi
    if [[ -d "$STASH_DIR" && -d "$NEW_DIR" && ! -d "$LIVE_DIR" ]]; then
      finish_cutover
      exit 0
    fi
    if [[ -d "$STASH_DIR" && -d "$LIVE_DIR" && -d "$NEW_DIR" ]]; then
      echo "Ambiguous: ${STASH_DIR}, ${LIVE_DIR}, and ${NEW_DIR} all exist. Move extras aside (do not delete) and re-run." >&2
      exit 1
    fi
    if [[ -d "$STASH_DIR" && -d "$LIVE_DIR" && ! -d "$NEW_DIR" ]]; then
      if [[ "$ver" == 26.8* ]]; then
        echo "Already on ${ver}."
        exit 0
      fi
      echo "Stash present; starting live as ${TARGET_VERSION}..."
      start_clickhouse clickhouse "$TARGET_VERSION"
      start_backup
      start_writers
      ver="$(running_version)"
      if [[ "$ver" == 26.8* ]]; then
        echo "Already migrated (${ver})."
        table_counts
        exit 0
      fi
      echo "Stash exists but live is ${ver:-unknown}. Rollback, or move ${STASH_DIR} aside to start over." >&2
      exit 1
    fi

    echo "Stopping writers..."
    stop_writers

    prefix="${BACKUP:-${2:-}}"
    if [[ -z "$prefix" && -f "$PREFIX_FILE" && -d "$NEW_DIR" ]]; then
      prefix="$(tr -d '[:space:]' < "$PREFIX_FILE")"
      echo "Resuming with saved prefix ${prefix}"
    fi
    if [[ -z "$prefix" && "$ver" == 25.10* ]]; then
      echo "Taking a last incremental..."
      "${DC[@]}" exec -T clickhouse-backup /bin/bash /opt/backup-loop.sh once incremental || true
      prefix="$(pick_backup_prefix)"
    fi
    if [[ -z "$prefix" && -f "$PREFIX_FILE" ]]; then
      prefix="$(tr -d '[:space:]' < "$PREFIX_FILE")"
      echo "Using saved prefix ${prefix}"
    fi
    if [[ -z "$prefix" ]]; then
      echo "No backup prefix. Pass BACKUP=inc-YYYY-MM-DD-HH or ensure backups exist." >&2
      exit 1
    fi
    prefix="${prefix%/}"
    printf '%s\n' "$prefix" > "$PREFIX_FILE"
    echo "Will restore ${prefix}"

    stop_ch
    mkdir -p "$NEW_DIR"
    chmod a+rwx "$NEW_DIR"

    start_clickhouse clickhouse-26.8 "$TARGET_VERSION"
    restore_backup "$prefix"

    echo "Re-running init so .env service users win..."
    "${DC[@]}" exec -T clickhouse bash /docker-entrypoint-initdb.d/init-clickhouse.sh

    echo "=== table counts on 26.8 (25.10 still at ${LIVE_DIR}) ==="
    table_counts
    "${DC[@]}" exec -T clickhouse clickhouse-client --query "SELECT version()"

    finish_cutover
    ;;
  rollback)
    need_confirm
    show_status
    echo "Rolling back to ${ROLLBACK_VERSION}..."
    stop_writers
    stop_ch
    if [[ -d "$STASH_DIR" ]]; then
      aside "$FAILED_DIR"
      if [[ -d "$LIVE_DIR" ]]; then
        mv "$LIVE_DIR" "$FAILED_DIR"
      fi
      mv "$STASH_DIR" "$LIVE_DIR"
    elif [[ -d "$NEW_DIR" ]]; then
      echo "Cutover never finished; 25.10 is still at ${LIVE_DIR}. Putting 26.8 aside."
      aside "$NEW_DIR"
    elif [[ -d "$LIVE_DIR" ]]; then
      echo "No ${STASH_DIR}. Starting ${ROLLBACK_VERSION} on existing ${LIVE_DIR}."
    else
      echo "No ClickHouse data dir to roll back to." >&2
      exit 1
    fi
    start_clickhouse clickhouse "$ROLLBACK_VERSION"
    start_backup
    start_writers
    "${DC[@]}" exec -T clickhouse clickhouse-client --query "SELECT version()"
    echo "✓ Rolled back to ${ROLLBACK_VERSION}."
    if [[ -d "$FAILED_DIR" ]]; then
      echo "  26.8 data kept at ${FAILED_DIR}"
    fi
    ;;
  help)
    echo "Usage:"
    echo "  make clickhouse migrate CONFIRM=1"
    echo "  make clickhouse rollback CONFIRM=1"
    ;;
  *)
    echo "Usage:" >&2
    echo "  make clickhouse migrate CONFIRM=1" >&2
    echo "  make clickhouse rollback CONFIRM=1" >&2
    exit 1
    ;;
esac
