#!/usr/bin/env bash
# ClickHouse BACKUP/RESTORE loop: daily full, hourly incremental.
# Backups are pushed to a remote S3 server (CLICKHOUSE_BACKUP_S3_*), ~100GB per full.
# Keep 2 completed fulls so a third can exist while the next full is writing; keep
# incrementals only for the newest full day.
# Uses BACKUP TO S3() (self-contained prefixes) so a new instance can RESTORE without local disk metadata.
# Usage:
#   backup-loop.sh                  # daemon (run once, then every hour)
#   backup-loop.sh once [auto|full|incremental]
#   backup-loop.sh restore <prefix> # e.g. inc-2026-09-15-14 or full-2026-09-15
#   backup-loop.sh status
#   backup-loop.sh list             # every object in the bucket, with sizes
#   backup-loop.sh prefixes         # completed backup prefixes only (have .backup), sorted
set -euo pipefail

BACKUP_DATABASES=(dlt envio zapalytics)
KEEP_FULLS="${CLICKHOUSE_BACKUP_KEEP_FULLS:-2}"
KEEP_INCREMENTAL_DAYS="${CLICKHOUSE_BACKUP_KEEP_INCREMENTAL_DAYS:-1}"
FULL_HOUR="${CLICKHOUSE_BACKUP_FULL_HOUR:-0}" # UTC hour for the daily full
CH_HOST="${CLICKHOUSE_BACKUP_CH_HOST:-clickhouse}"
CH_USER="${CLICKHOUSE_BACKUP_CH_USER:-backup}"
CH_PASSWORD="${CLICKHOUSE_BACKUP_PASSWORD:-${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD must be set}}"
S3_ENDPOINT="${CLICKHOUSE_BACKUP_S3_ENDPOINT:?CLICKHOUSE_BACKUP_S3_ENDPOINT must be set (remote S3 server)}"
S3_BUCKET="${CLICKHOUSE_BACKUP_S3_BUCKET:-clickhouse-backups}"
S3_ACCESS_KEY="${CLICKHOUSE_BACKUP_S3_ACCESS_KEY:?CLICKHOUSE_BACKUP_S3_ACCESS_KEY must be set}"
S3_SECRET_KEY="${CLICKHOUSE_BACKUP_S3_SECRET_KEY:?CLICKHOUSE_BACKUP_S3_SECRET_KEY must be set}"
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

backup_in_progress() {
  local prefix="$1"
  [[ "$(ch "SELECT count() FROM system.backups WHERE status = 'CREATING_BACKUP' AND position(name, '/${prefix}/') > 0")" != "0" ]]
}

# A prefix without .backup that no running BACKUP is writing is a leftover of an interrupted or
# failed backup (its .lock makes every retry fail with BACKUP_ALREADY_EXISTS): delete it.
remove_if_stale() {
  local prefix="$1"
  if prefix_exists "$prefix" || backup_in_progress "$prefix"; then
    return 0
  fi
  if [[ -n "$(mc_cmd ls "${MC_ALIAS}/${S3_BUCKET}/${prefix}/" 2>/dev/null)" ]]; then
    echo "Removing incomplete backup ${prefix} (no .backup marker, not being written)"
    mc_cmd rm --recursive --force "${MC_ALIAS}/${S3_BUCKET}/${prefix}"
  fi
}

run_backup() {
  local mode="$1"
  local today hour dest settings items
  today="$(date -u +%Y-%m-%d)"
  hour="$(date -u +%H)"

  if [[ "$mode" == "full" ]]; then
    dest="full-${today}"
  else
    dest="inc-${today}-${hour}"
    if ! prefix_exists "full-${today}"; then
      echo "No completed full backup for ${today}; taking a full instead of incremental."
      run_backup full
      return
    fi
  fi
  if prefix_exists "$dest"; then
    echo "Backup ${dest} already exists; skipping."
    return 0
  fi
  if backup_in_progress "$dest"; then
    echo "Backup ${dest} is still being written; skipping."
    return 0
  fi
  remove_if_stale "$dest"

  # ASYNC + polling: a ~100GB full to remote S3 outlives any client timeout, and a dropped
  # synchronous client cancels the BACKUP and leaves a half-written prefix behind.
  local id="${dest}-$(date -u +%s)"
  settings="id = '${id}'"
  if [[ "$mode" != "full" ]]; then
    settings+=", base_backup = $(s3_dest "full-${today}")"
  fi

  items="$(build_backup_items)"
  echo "Starting ${mode} backup to $(s3_url "$dest") (id ${id})"
  echo "  items: ${items}"
  local start status error
  start="$(date -u +%s)"
  ch "SET log_queries = 0; BACKUP ${items} TO $(s3_dest "$dest") SETTINGS ${settings} ASYNC" >/dev/null

  while true; do
    status="$(ch "SELECT status FROM system.backups WHERE id = '${id}'")"
    case "$status" in
      CREATING_BACKUP|"") sleep 30 ;;
      BACKUP_CREATED)
        echo "Backup ${dest} finished in $(( $(date -u +%s) - start ))s"
        return 0
        ;;
      *)
        error="$(ch "SELECT error FROM system.backups WHERE id = '${id}'")"
        echo "Backup ${dest} ${status} after $(( $(date -u +%s) - start ))s: ${error}" >&2
        return 1
        ;;
    esac
  done
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
  local prefix date_part inc_cutoff i
  local -a fulls=() incs=()

  echo "Pruning: keep ${KEEP_FULLS} newest completed fulls, incrementals for ${KEEP_INCREMENTAL_DAYS} newest day(s)"

  while IFS= read -r prefix; do
    [[ -z "$prefix" ]] && continue
    # Only completed backups count toward retention, so a half-written full never pushes out a good one.
    if ! prefix_exists "$prefix"; then
      remove_if_stale "$prefix" || true
      continue
    fi
    case "$prefix" in
      full-*) fulls+=("$prefix") ;;
      inc-*) incs+=("$prefix") ;;
    esac
  done < <(list_backup_prefixes)

  if ((${#fulls[@]} > 0)); then
    IFS=$'\n' fulls=($(printf '%s\n' "${fulls[@]}" | sort -r))
    unset IFS
  fi

  # Drop incrementals first so a full is never kept only because an old inc still points at it.
  inc_cutoff="$(date -u -d "-$((KEEP_INCREMENTAL_DAYS - 1)) days" +%Y-%m-%d 2>/dev/null \
    || date -u -v-"$((KEEP_INCREMENTAL_DAYS - 1))"d +%Y-%m-%d)"
  for prefix in "${incs[@]+"${incs[@]}"}"; do
    date_part="$(echo "$prefix" | sed -E 's/^inc-([0-9]{4}-[0-9]{2}-[0-9]{2})-.*/\1/')"
    if [[ "$date_part" < "$inc_cutoff" ]]; then
      echo "Removing old incremental ${prefix}"
      mc_cmd rm --recursive --force "${MC_ALIAS}/${S3_BUCKET}/${prefix}" || true
    fi
  done

  if ((${#fulls[@]} > KEEP_FULLS)); then
    for ((i = KEEP_FULLS; i < ${#fulls[@]}; i++)); do
      echo "Removing old full ${fulls[i]}"
      mc_cmd rm --recursive --force "${MC_ALIAS}/${S3_BUCKET}/${fulls[i]}" || true
    done
  fi
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

list_files() {
  echo "=== ${S3_BUCKET} (all objects) ==="
  mc_cmd ls --recursive --summarize "${MC_ALIAS}/${S3_BUCKET}/"
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
  list)
    list_files
    ;;
  prefixes)
    # .backup is written last, so a prefix without it is still being written (or failed).
    while IFS= read -r prefix; do
      if [[ -n "$prefix" ]] && prefix_exists "$prefix"; then
        echo "$prefix"
      fi
    done < <(list_backup_prefixes | sort)
    ;;
  daemon)
    wait_for_clickhouse
    echo "Backup loop starting (daily full at ${FULL_HOUR}:00 UTC, hourly incremental, keep ${KEEP_FULLS} fulls + ${KEEP_INCREMENTAL_DAYS}d incrementals)"
    run_backup "$(decide_mode)" || echo "Initial backup failed; will retry next hour" >&2
    prune_old_backups || echo "Initial prune failed; will retry next hour" >&2
    while true; do
      sleep_until_next_hour
      run_backup "$(decide_mode)" || echo "Backup failed; will retry next hour" >&2
      prune_old_backups || echo "Prune failed; will retry next hour" >&2
    done
    ;;
  *)
    echo "Usage: backup-loop.sh [daemon|once [auto|full|incremental]|restore <prefix>|status|list|prefixes]" >&2
    exit 1
    ;;
esac
