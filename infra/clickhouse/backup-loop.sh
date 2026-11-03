#!/usr/bin/env bash
# ClickHouse BACKUP/RESTORE loop: daily full, hourly incremental.
# Backups land in remote S3 (CLICKHOUSE_BACKUP_S3_*), ~100GB per full, as self-contained prefixes
# so a new instance can RESTORE FROM S3() without local disk metadata.
# Two steps: BACKUP to the local staging disk (fast, nothing to time out), then upload with mc mirror,
# which retries and skips what is already uploaded. .backup goes up last, so a prefix is only ever
# complete remotely once everything else is there. A failed upload is resumed on the next run.
# Keep 2 completed fulls so a third can exist while the next full is writing; keep
# incrementals only for the newest full day.
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
# Local staging disk: <path> of the "backups" disk in config.d/50-backups.xml, mounted here too
STAGING_DISK="${CLICKHOUSE_BACKUP_STAGING_DISK:-backups}"
STAGING_DIR="${CLICKHOUSE_BACKUP_STAGING_DIR:-/backups}"
# Parallel uploads; Hetzner answers 504 when flooded
MIRROR_WORKERS="${CLICKHOUSE_BACKUP_MIRROR_WORKERS:-8}"

s3_url() {
  local prefix="$1"
  echo "${S3_ENDPOINT%/}/${S3_BUCKET}/${prefix}/"
}

s3_dest() {
  local prefix="$1"
  # log_queries=0 keeps keys out of system.query_log
  echo "S3('$(s3_url "$prefix")', '${S3_ACCESS_KEY}', '${S3_SECRET_KEY}')"
}

local_dest() {
  local prefix="$1"
  echo "Disk('${STAGING_DISK}', '${prefix}/')"
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
  # Matches both Disk('backups', '<prefix>/') and S3('.../<prefix>/')
  [[ "$(ch "SELECT count() FROM system.backups WHERE status = 'CREATING_BACKUP' AND position(name, '${prefix}/') > 0")" != "0" ]]
}

staged_complete() {
  local prefix="$1"
  [[ -f "${STAGING_DIR}/${prefix}/.backup" ]]
}

prepare_staging() {
  mkdir -p "$STAGING_DIR"
  # ClickHouse writes here as its own user; the bind mount starts out root-owned.
  chown clickhouse:clickhouse "$STAGING_DIR" 2>/dev/null \
    || echo "Could not chown ${STAGING_DIR} to clickhouse; BACKUP may fail to write" >&2
}

# One run at a time: the upload happens outside ClickHouse, so system.backups cannot tell a
# one-shot `once` that the daemon is mid-upload (and prune would delete the half-uploaded prefix).
with_lock() {
  if ! command -v flock >/dev/null 2>&1; then
    "$@"
    return
  fi
  local rc=0
  exec 9>"${STAGING_DIR}/.lock"
  if ! flock -n 9; then
    echo "Another backup run is in progress; skipping."
    exec 9>&-
    return 0
  fi
  "$@" || rc=$?
  exec 9>&-
  return "$rc"
}

# Hetzner object storage fails mc's bulk delete (`rm --recursive`: "unexpected EOF"),
# so fall back to deleting object by object, then check nothing is left.
remove_prefix() {
  local prefix="$1"
  local url="${MC_ALIAS}/${S3_BUCKET}/${prefix}"
  if mc_cmd rm --recursive --force "$url" >/dev/null 2>&1; then
    return 0
  fi
  echo "Bulk delete of ${prefix} failed; deleting object by object"
  # Single deletes also hit sporadic "unexpected EOF"; retry what is left a few times.
  local attempt
  for attempt in 1 2 3 4 5; do
    mc_cmd ls --recursive "${url}/" 2>/dev/null | awk '{print $NF}' \
      | xargs -P 16 -I{} "$MC_BIN" rm --force "${url}/{}" >/dev/null 2>&1 || true
    if [[ -z "$(mc_cmd ls "${url}/" 2>/dev/null)" ]]; then
      return 0
    fi
    sleep $((attempt * 10))
  done
  echo "Could not delete ${prefix}: objects remain after 5 attempts" >&2
  return 1
}

# A prefix without .backup that no running BACKUP is writing is a leftover of an interrupted or
# failed backup (its .lock makes every retry fail with BACKUP_ALREADY_EXISTS): delete it.
remove_if_stale() {
  local prefix="$1"
  # A complete local copy means the upload is pending: keep what is already up there.
  if prefix_exists "$prefix" || staged_complete "$prefix" || backup_in_progress "$prefix"; then
    return 0
  fi
  if [[ -n "$(mc_cmd ls "${MC_ALIAS}/${S3_BUCKET}/${prefix}/" 2>/dev/null)" ]]; then
    echo "Removing incomplete backup ${prefix} (no .backup marker, not being written)"
    remove_prefix "$prefix"
  fi
}

# Refuse to start a backup that could fill the disk ClickHouse itself lives on.
enough_staging_space() {
  local mode="$1"
  local free_kb need_kb data_kb in_list="" db
  free_kb="$(df -Pk "$STAGING_DIR" | awk 'NR == 2 {print $4}')"
  for db in "${BACKUP_DATABASES[@]}"; do
    [[ -n "$in_list" ]] && in_list+=", "
    in_list+="'${db}'"
  done
  data_kb="$(ch "SELECT intDiv(sum(bytes_on_disk), 1024) FROM system.parts WHERE active AND database IN (${in_list})")"
  if [[ "$mode" == "full" ]]; then
    need_kb=$(( data_kb + data_kb / 5 )) # full copy + 20% headroom for data written meanwhile
  else
    need_kb=$(( data_kb / 5 )) # incrementals are small, but a big merge can rewrite a lot
  fi
  if (( free_kb < need_kb )); then
    echo "Not enough free space in ${STAGING_DIR} for a ${mode} backup: $(( free_kb / 1048576 ))GiB free, need ~$(( need_kb / 1048576 ))GiB" >&2
    return 1
  fi
}

# Upload a completed local backup; mirror skips objects already there, so retries only send the rest.
upload_prefix() {
  local prefix="$1"
  local src="${STAGING_DIR}/${prefix}"
  local url="${MC_ALIAS}/${S3_BUCKET}/${prefix}"
  local attempt start size
  size="$(du -sh "$src" 2>/dev/null | cut -f1)"
  start="$(date -u +%s)"
  for attempt in 1 2 3 4 5; do
    echo "Uploading ${prefix} (${size}) to $(s3_url "$prefix") (attempt ${attempt}/5)"
    if mc_cmd mirror --overwrite --quiet --max-workers "$MIRROR_WORKERS" --exclude ".backup" "${src}/" "${url}/" >/dev/null \
      && mc_cmd cp --quiet "${src}/.backup" "${url}/.backup" >/dev/null \
      && prefix_exists "$prefix"; then
      echo "Upload of ${prefix} finished in $(( $(date -u +%s) - start ))s"
      # Left over from the old direct-to-S3 BACKUP; harmless but confusing.
      mc_cmd rm --force "${url}/.lock" >/dev/null 2>&1 || true
      rm -rf "$src"
      return 0
    fi
    echo "Upload attempt ${attempt} of ${prefix} failed" >&2
    sleep $((attempt * 60))
  done
  echo "Upload of ${prefix} failed after 5 attempts; local copy kept in ${src}, resumed next run" >&2
  return 1
}

# Local staging shares the disk with ClickHouse data, so nothing may linger there:
# - uploaded backups are deleted by upload_prefix,
# - failed/interrupted local BACKUPs are deleted here,
# - a completed backup whose upload failed is retried here; once it is from an earlier day, a newer
#   full supersedes it and it is dropped instead.
# A pending upload blocks new backups (run_backup), so at most one backup is ever staged.
finish_staged() {
  local dir prefix date_part today
  today="$(date -u +%Y-%m-%d)"
  for dir in "${STAGING_DIR}"/*/; do
    [[ -d "$dir" ]] || continue
    prefix="$(basename "$dir")"
    [[ "$prefix" =~ ^(full|inc)- ]] || continue
    if backup_in_progress "$prefix"; then
      continue
    fi
    if ! staged_complete "$prefix"; then
      echo "Removing incomplete local backup ${prefix}"
      rm -rf "$dir"
    elif prefix_exists "$prefix"; then
      rm -rf "$dir"
    else
      date_part="$(echo "$prefix" | sed -E 's/^(full|inc)-([0-9]{4}-[0-9]{2}-[0-9]{2}).*/\2/')"
      if [[ "$date_part" < "$today" ]]; then
        echo "Dropping local backup ${prefix}: never uploaded and superseded by today's backups" >&2
        rm -rf "$dir"
        continue
      fi
      echo "Resuming upload of ${prefix}"
      upload_prefix "$prefix" || return 1
    fi
  done
}

# Poll an ASYNC BACKUP until it is done.
wait_for_backup() {
  local id="$1" dest="$2" start="$3"
  local status error missing=0
  while true; do
    status="$(ch "SELECT status FROM system.backups WHERE id = '${id}'" || echo QUERY_FAILED)"
    case "$status" in
      CREATING_BACKUP) missing=0; sleep 30 ;;
      ""|QUERY_FAILED)
        # Not listed (yet) or ClickHouse briefly unreachable; give up after ~10 minutes.
        missing=$((missing + 1))
        if ((missing > 20)); then
          echo "Backup ${dest} (id ${id}) not found in system.backups; giving up" >&2
          return 1
        fi
        sleep 30
        ;;
      BACKUP_CREATED)
        echo "Local backup ${dest} finished in $(( $(date -u +%s) - start ))s"
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

run_backup() {
  local mode="$1"
  local today hour dest settings items
  today="$(date -u +%Y-%m-%d)"
  hour="$(date -u +%H)"

  # Explicit checks throughout: the daemon calls run_backup in `|| ...`, where set -e is off.
  # Pending uploads first: an incremental needs today's full complete remotely.
  if ! finish_staged; then
    echo "Pending upload failed; not starting a new backup." >&2
    return 1
  fi

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
  rm -rf "${STAGING_DIR:?}/${dest}"
  if ! enough_staging_space "$mode"; then
    return 1
  fi

  # ASYNC + polling: a ~100GB full outlives any client timeout, and a dropped synchronous
  # client cancels the BACKUP.
  local id="${dest}-$(date -u +%s)"
  settings="id = '${id}'"
  if [[ "$mode" != "full" ]]; then
    # Base read from remote S3 (only its .backup metadata), so the incremental restores from S3 alone.
    settings+=", base_backup = $(s3_dest "full-${today}")"
  fi

  items="$(build_backup_items)"
  echo "Starting ${mode} backup to ${STAGING_DIR}/${dest} (id ${id})"
  echo "  items: ${items}"
  local start
  start="$(date -u +%s)"
  # max_execution_time = 0: the default profile caps queries at 300s, ASYNC backups included.
  if ! ch "SET log_queries = 0; SET max_execution_time = 0; BACKUP ${items} TO $(local_dest "$dest") SETTINGS ${settings} ASYNC" >/dev/null; then
    echo "Failed to start backup ${dest}" >&2
    return 1
  fi
  if ! wait_for_backup "$id" "$dest" "$start"; then
    rm -rf "${STAGING_DIR:?}/${dest}"
    return 1
  fi
  upload_prefix "$dest"
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
      remove_prefix "$prefix" || true
    fi
  done

  if ((${#fulls[@]} > KEEP_FULLS)); then
    for ((i = KEEP_FULLS; i < ${#fulls[@]}; i++)); do
      echo "Removing old full ${fulls[i]}"
      remove_prefix "${fulls[i]}" || true
    done
  fi
}

show_status() {
  echo "=== system.backups (latest 20) ==="
  ch "SELECT start_time, status, name, substring(error, 1, 200) AS error, formatReadableSize(total_size) AS size FROM system.backups ORDER BY start_time DESC LIMIT 20 FORMAT PrettyCompact"
  echo
  echo "=== Local staging (${STAGING_DIR}; complete ones are waiting for upload) ==="
  du -sh "${STAGING_DIR}"/*/ 2>/dev/null || echo "(empty)"
  echo
  echo "=== S3 prefixes (${S3_BUCKET}) ==="
  list_backup_prefixes || echo "(none)"
}

restore_from() {
  local prefix="${1:?restore prefix required, e.g. inc-YYYY-MM-DD-HH}"
  prefix="${prefix%/}"
  echo "RESTORE ALL FROM $(s3_url "$prefix")"
  ch "SET log_queries = 0; SET max_execution_time = 0; RESTORE ALL FROM $(s3_dest "$prefix")"
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

run_once() {
  local rc=0
  run_backup "$1" || rc=$?
  prune_old_backups || rc=$?
  return "$rc"
}

run_hourly() {
  run_backup "$(decide_mode)" || echo "Backup failed; will retry next hour" >&2
  prune_old_backups || echo "Prune failed; will retry next hour" >&2
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
    prepare_staging
    with_lock run_once "$mode"
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
    prepare_staging
    while true; do
      with_lock run_hourly || true
      sleep_until_next_hour
    done
    ;;
  *)
    echo "Usage: backup-loop.sh [daemon|once [auto|full|incremental]|restore <prefix>|status|list|prefixes]" >&2
    exit 1
    ;;
esac
