#!/usr/bin/env bash
# ClickHouse BACKUP/RESTORE loop: daily full, hourly incremental, 7-day prune.
# Uses BACKUP TO S3() (self-contained prefixes) so a new instance can RESTORE without local disk metadata.
# Usage:
#   backup-loop.sh                  # daemon (run once, then every hour)
#   backup-loop.sh once [auto|full|incremental]
#   backup-loop.sh restore <prefix> # e.g. inc-2026-09-15-14 or full-2026-09-15
#   backup-loop.sh status
set -euo pipefail

BACKUP_DATABASES=(dlt envio zapalytics dev_fron dev_chebin)
RETENTION_DAYS="${CLICKHOUSE_BACKUP_RETENTION_DAYS:-7}"
FULL_HOUR="${CLICKHOUSE_BACKUP_FULL_HOUR:-0}" # UTC hour for the daily full
CH_HOST="${CLICKHOUSE_BACKUP_CH_HOST:-clickhouse}"
CH_USER="${CLICKHOUSE_BACKUP_CH_USER:-backup}"
CH_PASSWORD="${CLICKHOUSE_BACKUP_PASSWORD:-${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD must be set}}"
S3_ENDPOINT="${CLICKHOUSE_BACKUP_S3_ENDPOINT:-http://rustfs:9000}"
S3_BUCKET="${CLICKHOUSE_BACKUP_S3_BUCKET:-clickhouse-backups}"
S3_ACCESS_KEY="${CLICKHOUSE_BACKUP_S3_ACCESS_KEY:-${RUSTFS_ACCESS_KEY:-admin}}"
S3_SECRET_KEY="${CLICKHOUSE_BACKUP_S3_SECRET_KEY:-${RUSTFS_SECRET_KEY:?CLICKHOUSE_BACKUP_S3_SECRET_KEY or RUSTFS_SECRET_KEY must be set}}"
MC_BIN="${MC_BIN:-/usr/local/bin/mc}"
MC_ALIAS="chbackups"

s3_url() {
  local prefix="$1"
  echo "${S3_ENDPOINT%/}/${S3_BUCKET}/${prefix}/"
}

s3_dest() {
  local prefix="$1"
  # log_queries=0 keeps keys out of system.query_log
  echo "S3('$(s3_url "$prefix")', '${S3_ACCESS_KEY}', '${S3_SECRET_KEY}')"
}

ch() {
  clickhouse-client \
    --host "$CH_HOST" \
    --user "$CH_USER" \
    --password "$CH_PASSWORD" \
    --receive_timeout 14400 \
    --send_timeout 14400 \
    --multiquery \
    --query "$1"
}

wait_for_clickhouse() {
  local i
  for i in $(seq 1 60); do
    if ch "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    echo "Waiting for ClickHouse at ${CH_HOST} (${i}/60)..."
    sleep 2
  done
  echo "ClickHouse is not reachable at ${CH_HOST}" >&2
  return 1
}

ensure_mc() {
  if [[ -x "$MC_BIN" ]]; then
    return 0
  fi
  echo "mc not found at ${MC_BIN}; build infra/clickhouse/Dockerfile.backup" >&2
  return 1
}

mc_cmd() {
  ensure_mc
  "$MC_BIN" alias set "$MC_ALIAS" "$S3_ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY" >/dev/null
  "$MC_BIN" "$@"
}

list_backup_prefixes() {
  mc_cmd ls "${MC_ALIAS}/${S3_BUCKET}/" 2>/dev/null | awk '{print $NF}' | sed 's:/$::' | grep -E '^(full|inc)-' || true
}

prefix_exists() {
  local prefix="$1"
  mc_cmd stat "${MC_ALIAS}/${S3_BUCKET}/${prefix}/.backup" >/dev/null 2>&1
}

existing_backup_databases() {
  local in_list="" db
  for db in "${BACKUP_DATABASES[@]}"; do
    if [[ -n "$in_list" ]]; then
      in_list+=", "
    fi
    in_list+="'${db}'"
  done
  ch "SELECT name FROM system.databases WHERE name IN (${in_list}) ORDER BY name"
}

build_backup_items() {
  local items="TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas"
  local db
  while IFS= read -r db; do
    [[ -z "$db" ]] && continue
    items+=", DATABASE ${db}"
  done < <(existing_backup_databases)
  echo "$items"
}

run_backup() {
  local mode="$1"
  local today hour dest extra items
  today="$(date -u +%Y-%m-%d)"
  hour="$(date -u +%H)"
  extra=""

  if [[ "$mode" == "full" ]]; then
    dest="full-${today}"
    if prefix_exists "$dest"; then
      echo "Full backup ${dest} already exists; skipping."
      return 0
    fi
  else
    dest="inc-${today}-${hour}"
    if ! prefix_exists "full-${today}"; then
      echo "No full backup for ${today}; taking a full instead of incremental."
      run_backup full
      return 0
    fi
    if prefix_exists "$dest"; then
      echo "Incremental ${dest} already exists; skipping."
      return 0
    fi
    extra=" SETTINGS base_backup = $(s3_dest "full-${today}")"
  fi

  items="$(build_backup_items)"
  echo "Starting ${mode} backup to $(s3_url "$dest")"
  echo "  items: ${items}"
  local start status
  start="$(date -u +%s)"
  set +e
  local out
  out="$(ch "SET log_queries = 0; BACKUP ${items} TO $(s3_dest "$dest")${extra}" 2>&1)"
  local rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    if echo "$out" | grep -q BACKUP_ALREADY_EXISTS; then
      echo "Backup ${dest} already exists; skipping."
      return 0
    fi
    echo "$out" >&2
    return "$rc"
  fi
  echo "$out"
  status="$(ch "SELECT status FROM system.backups ORDER BY start_time DESC LIMIT 1")"
  echo "Backup ${dest} finished in $(( $(date -u +%s) - start ))s (system.backups status=${status:-unknown})"
}

decide_mode() {
  local hour today
  hour="$(date -u +%H)"
  hour=$((10#$hour))
  today="$(date -u +%Y-%m-%d)"
  if [[ "$hour" -eq "$FULL_HOUR" ]] || ! prefix_exists "full-${today}"; then
    echo full
  else
    echo incremental
  fi
}

prune_old_backups() {
  local cutoff prefix date_part
  cutoff="$(date -u -d "-${RETENTION_DAYS} days" +%Y-%m-%d 2>/dev/null || date -u -v-"${RETENTION_DAYS}"d +%Y-%m-%d)"
  echo "Pruning backup prefixes older than ${cutoff} (keep ${RETENTION_DAYS} days; keep fulls still referenced by incrementals)"

  local -a inc_dates=()
  while IFS= read -r prefix; do
    [[ -z "$prefix" ]] && continue
    if [[ "$prefix" == inc-* ]]; then
      date_part="$(echo "$prefix" | sed -E 's/^inc-([0-9]{4}-[0-9]{2}-[0-9]{2})-.*/\1/')"
      if [[ "$date_part" < "$cutoff" ]]; then
        echo "Removing old incremental ${prefix}"
        mc_cmd rm --recursive --force "${MC_ALIAS}/${S3_BUCKET}/${prefix}" || true
      else
        inc_dates+=("$date_part")
      fi
    fi
  done < <(list_backup_prefixes)

  while IFS= read -r prefix; do
    [[ -z "$prefix" ]] && continue
    if [[ "$prefix" == full-* ]]; then
      date_part="${prefix#full-}"
      if [[ "$date_part" < "$cutoff" ]]; then
        local referenced=0 d
        for d in "${inc_dates[@]+"${inc_dates[@]}"}"; do
          if [[ "$d" == "$date_part" ]]; then
            referenced=1
            break
          fi
        done
        if [[ "$referenced" -eq 1 ]]; then
          echo "Keeping full ${prefix} (incrementals still reference it)"
        else
          echo "Removing old full ${prefix}"
          mc_cmd rm --recursive --force "${MC_ALIAS}/${S3_BUCKET}/${prefix}" || true
        fi
      fi
    fi
  done < <(list_backup_prefixes)
}

show_status() {
  echo "=== system.backups (latest 20) ==="
  ch "SELECT start_time, status, name, error, formatReadableSize(total_size) AS size FROM system.backups ORDER BY start_time DESC LIMIT 20 FORMAT PrettyCompact"
  echo
  echo "=== S3 prefixes (${S3_BUCKET}) ==="
  list_backup_prefixes || echo "(none)"
}

restore_from() {
  local prefix="${1:?restore prefix required, e.g. inc-YYYY-MM-DD-HH}"
  prefix="${prefix%/}"
  echo "RESTORE ALL FROM $(s3_url "$prefix")"
  ch "SET log_queries = 0; RESTORE ALL FROM $(s3_dest "$prefix")"
  echo "Restore finished. Re-run init-clickhouse.sh (or restart ClickHouse with ALWAYS_RUN_INITDB_SCRIPTS) so .env service users win."
}

sleep_until_next_hour() {
  local now next
  now="$(date -u +%s)"
  next=$(( (now / 3600 + 1) * 3600 ))
  echo "Sleeping $(( next - now ))s until next UTC hour..."
  sleep $(( next - now ))
}

cmd="${1:-daemon}"
case "$cmd" in
  once)
    wait_for_clickhouse
    mode="${2:-auto}"
    if [[ "$mode" == "auto" ]]; then
      mode="$(decide_mode)"
    fi
    if [[ "$mode" != "full" && "$mode" != "incremental" ]]; then
      echo "Usage: backup-loop.sh once [auto|full|incremental]" >&2
      exit 1
    fi
    run_backup "$mode"
    prune_old_backups
    ;;
  restore)
    wait_for_clickhouse
    restore_from "${2:-}"
    ;;
  status)
    wait_for_clickhouse
    show_status
    ;;
  daemon)
    wait_for_clickhouse
    echo "Backup loop starting (daily full at ${FULL_HOUR}:00 UTC, hourly incremental, retain ${RETENTION_DAYS} days)"
    run_backup "$(decide_mode)" || echo "Initial backup failed; will retry next hour" >&2
    prune_old_backups || echo "Initial prune failed; will retry next hour" >&2
    while true; do
      sleep_until_next_hour
      run_backup "$(decide_mode)" || echo "Backup failed; will retry next hour" >&2
      prune_old_backups || echo "Prune failed; will retry next hour" >&2
    done
    ;;
  *)
    echo "Usage: backup-loop.sh [daemon|once [auto|full|incremental]|restore <prefix>|status]" >&2
    exit 1
    ;;
esac
