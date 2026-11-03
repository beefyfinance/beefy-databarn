#!/usr/bin/env bash
# Restore a prod ClickHouse backup into local ClickHouse from the remote S3 server prod pushes to.
#
# Runs inside the local clickhouse-backup toolbox container (mc + clickhouse-client): downloads the
# backup to the local "backups" disk with mc mirror (retries, resumes), then RESTOREs from Disk().
# RESTORE straight from S3() dies on the remote's sporadic 504 GatewayTimeouts.
# Needs local Docker disk space for the backup (~80GB for a full); the download is deleted after a
# successful restore and kept (resumable) after a failed one.
# Only the data databases are restored (not system.users etc.), so local service users/passwords stay as-is.
# Read-only on the remote bucket: nothing is pushed or pruned.
#
# Usage:
#   infra/dev/pull-prod-backup.sh list             # list completed prod backup prefixes
#   infra/dev/pull-prod-backup.sh inspect <prefix> # top-level objects of a backup + .backup marker check
#   infra/dev/pull-prod-backup.sh [<prefix>] [-y]  # restore (default: newest full-*)
#
# Env (from .env):
#   CLICKHOUSE_BACKUP_S3_ENDPOINT / _BUCKET / _ACCESS_KEY / _SECRET_KEY  remote backup bucket (same as prod)
#   PROD_SSH_HOST   if the S3 values above are unset, read them from the prod backup container over SSH
#   PROD_REPO_DIR   repo checkout on the prod host (default: ~/beefy-databarn)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

S3_ENDPOINT="${CLICKHOUSE_BACKUP_S3_ENDPOINT:-}"
S3_BUCKET="${CLICKHOUSE_BACKUP_S3_BUCKET:-}"
S3_ACCESS_KEY="${CLICKHOUSE_BACKUP_S3_ACCESS_KEY:-}"
S3_SECRET_KEY="${CLICKHOUSE_BACKUP_S3_SECRET_KEY:-}"
PROD_REPO_DIR="${PROD_REPO_DIR:-~/beefy-databarn}"
DC=(docker compose -f "$ROOT_DIR/infra/dev/docker-compose.yml" --env-file "$ROOT_DIR/.env")

load_s3_config() {
  if [[ -n "$S3_ENDPOINT" && -n "$S3_ACCESS_KEY" && -n "$S3_SECRET_KEY" ]]; then
    S3_BUCKET="${S3_BUCKET:-clickhouse-backups}"
    return 0
  fi
  if [[ -z "${PROD_SSH_HOST:-}" ]]; then
    echo "Set CLICKHOUSE_BACKUP_S3_{ENDPOINT,ACCESS_KEY,SECRET_KEY} (same as prod) or PROD_SSH_HOST in .env" >&2
    exit 1
  fi
  echo "Reading remote backup bucket config from the prod backup container on ${PROD_SSH_HOST}..."
  local prod_dc="docker compose -f ${PROD_REPO_DIR}/infra/prod/docker-compose.yml --env-file ${PROD_REPO_DIR}/.env"
  local env_lines
  # Never echoed: values only land in shell variables.
  env_lines="$(ssh -o BatchMode=yes "$PROD_SSH_HOST" "${prod_dc} exec -T clickhouse-backup env" \
    | grep -E '^CLICKHOUSE_BACKUP_S3_(ENDPOINT|BUCKET|ACCESS_KEY|SECRET_KEY)=')"
  S3_ENDPOINT="$(sed -n 's/^CLICKHOUSE_BACKUP_S3_ENDPOINT=//p' <<<"$env_lines")"
  S3_BUCKET="$(sed -n 's/^CLICKHOUSE_BACKUP_S3_BUCKET=//p' <<<"$env_lines")"
  S3_ACCESS_KEY="$(sed -n 's/^CLICKHOUSE_BACKUP_S3_ACCESS_KEY=//p' <<<"$env_lines")"
  S3_SECRET_KEY="$(sed -n 's/^CLICKHOUSE_BACKUP_S3_SECRET_KEY=//p' <<<"$env_lines")"
  S3_BUCKET="${S3_BUCKET:-clickhouse-backups}"
  if [[ -z "$S3_ENDPOINT" || -z "$S3_ACCESS_KEY" || -z "$S3_SECRET_KEY" ]]; then
    echo "Prod backup container has no complete CLICKHOUSE_BACKUP_S3_* config" >&2
    exit 1
  fi
}

ensure_local_stack() {
  "${DC[@]}" up -d clickhouse clickhouse-backup >/dev/null
}

# Run a bash snippet in the local toolbox container with the remote bucket config in its env.
in_toolbox() {
  local script="$1"
  shift
  "${DC[@]}" exec -T \
    -e CLICKHOUSE_BACKUP_S3_ENDPOINT="$S3_ENDPOINT" \
    -e CLICKHOUSE_BACKUP_S3_BUCKET="$S3_BUCKET" \
    -e CLICKHOUSE_BACKUP_S3_ACCESS_KEY="$S3_ACCESS_KEY" \
    -e CLICKHOUSE_BACKUP_S3_SECRET_KEY="$S3_SECRET_KEY" \
    clickhouse-backup bash -c "$script" _ "$@"
}

list_prefixes() {
  in_toolbox '/bin/bash /opt/backup-loop.sh prefixes'
}

backup_databases() {
  in_toolbox 'mc alias set remote "$CLICKHOUSE_BACKUP_S3_ENDPOINT" "$CLICKHOUSE_BACKUP_S3_ACCESS_KEY" "$CLICKHOUSE_BACKUP_S3_SECRET_KEY" >/dev/null &&
    mc ls "remote/$CLICKHOUSE_BACKUP_S3_BUCKET/$1/metadata/"' "$1" \
    | awk '{print $NF}' | grep -E '\.sql$' | sed 's/\.sql$//' \
    | grep -vE '^(system|INFORMATION_SCHEMA|information_schema)$' || true
}

# clickhouse-client echoes the failing query (with the S3 secret) on error, so scrub it from output.
redact() {
  local secret="$S3_SECRET_KEY"
  secret="${secret//\\/\\\\}"
  secret="${secret//\//\\/}"
  secret="${secret//&/\\&}"
  sed "s/${secret}/[REDACTED]/g"
}

inspect_prefix() {
  in_toolbox 'mc alias set remote "$CLICKHOUSE_BACKUP_S3_ENDPOINT" "$CLICKHOUSE_BACKUP_S3_ACCESS_KEY" "$CLICKHOUSE_BACKUP_S3_SECRET_KEY" >/dev/null
    echo "== mc ls remote/$CLICKHOUSE_BACKUP_S3_BUCKET/$1/"
    mc ls "remote/$CLICKHOUSE_BACKUP_S3_BUCKET/$1/"
    echo "== mc stat remote/$CLICKHOUSE_BACKUP_S3_BUCKET/$1/.backup"
    mc stat "remote/$CLICKHOUSE_BACKUP_S3_BUCKET/$1/.backup"
    true' "$1" 2>&1 | redact
}

ch_local() {
  in_toolbox 'clickhouse-client --host "$CLICKHOUSE_BACKUP_CH_HOST" --user backup \
    --password "${CLICKHOUSE_BACKUP_PASSWORD:-$CLICKHOUSE_PASSWORD}" \
    --receive_timeout 14400 --send_timeout 14400 --multiquery --query "$1"' "$1" \
    2> >(redact >&2) | redact
}

# Download a backup to the local staging disk, object by object, 8 at a time. mc mirror stops at the
# first failed object and the remote fails some at random ("unexpected EOF", 504), so it barely moves.
# Each object lands in <name>.part and is renamed once complete, so a present file is a whole file and
# re-runs (and later rounds) only fetch what is missing. Rounds continue while they make progress.
download_prefix() {
  in_toolbox 'mc alias set remote "$CLICKHOUSE_BACKUP_S3_ENDPOINT" "$CLICKHOUSE_BACKUP_S3_ACCESS_KEY" "$CLICKHOUSE_BACKUP_S3_SECRET_KEY" >/dev/null || exit 1
    export src="remote/$CLICKHOUSE_BACKUP_S3_BUCKET/$1/" dst="/backups/$1/"
    mkdir -p "$dst"
    keys="/backups/.$1.keys"
    mc ls --recursive "$src" | awk "{print \$NF}" > "$keys" || exit 1
    total=$(wc -l < "$keys")
    [ "$total" -gt 0 ] || { echo "No objects under $src" >&2; exit 1; }
    find "$dst" -name "*.part" -delete
    round=0 stalled=0 start=$(date +%s)
    while [ "$stalled" -lt 3 ]; do
      round=$((round + 1))
      missing="$(while read -r k; do [ -f "$dst$k" ] || echo "$k"; done < "$keys")"
      left=$(printf "%s" "$missing" | grep -c . || true)
      if [ "$left" -eq 0 ]; then
        rm -f "$keys"
        chown -R clickhouse:clickhouse /backups
        echo "Download of $1 finished in $(( $(date +%s) - start ))s ($(du -sh "$dst" | cut -f1), $total files)"
        exit 0
      fi
      echo "Downloading $1: $((total - left))/$total files here, fetching $left (round $round)"
      printf "%s\n" "$missing" | xargs -P 8 -n 1 sh -c '\''mkdir -p "$(dirname "$dst$1")" && mc cp --quiet "$src$1" "$dst$1.part" >/dev/null 2>&1 && mv "$dst$1.part" "$dst$1" || rm -f "$dst$1.part"'\'' _''
      now_left="$(while read -r k; do [ -f "$dst$k" ] || echo "$k"; done < "$keys" | grep -c . || true)"
      if [ "$now_left" -lt "$left" ]; then stalled=0; else stalled=$((stalled + 1)); sleep $((stalled * 60)); fi
    done
    echo "Download of $1 stalled; partial copy kept in $dst (re-run to resume)" >&2
    exit 1' "$1" 2>&1 | redact # pipefail + set -e: a failed download stops here, before any DROP
}

remove_download() {
  in_toolbox 'rm -rf "/backups/$1"' "$1"
}

restore_prefix() {
  local prefix="$1" assume_yes="$2"
  local -a dbs=()
  local db items=""
  while IFS= read -r db; do
    [[ -n "$db" ]] && dbs+=("$db")
  done < <(backup_databases "$prefix")
  ((${#dbs[@]} > 0)) || { echo "No databases found in ${S3_BUCKET}/${prefix}/metadata" >&2; exit 1; }

  echo "Will DROP and restore local databases from ${prefix}: ${dbs[*]}"
  if [[ "$assume_yes" != "1" ]]; then
    read -r -p "Continue? [y/N] " answer
    [[ "$answer" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
  fi

  # Download first: local databases are only dropped once the whole backup is here.
  # An incremental's metadata points at its base full on S3; download that too and override it.
  local base="" settings=""
  if [[ "$prefix" == inc-* ]]; then
    base="full-$(sed -E 's/^inc-([0-9]{4}-[0-9]{2}-[0-9]{2}).*/\1/' <<<"$prefix")"
    download_prefix "$base"
    settings=" SETTINGS base_backup = Disk('backups', '${base}/')"
  fi
  download_prefix "$prefix"

  for db in "${dbs[@]}"; do
    ch_local "DROP DATABASE IF EXISTS \`${db}\` SYNC"
    [[ -n "$items" ]] && items+=", "
    items+="DATABASE \`${db}\`"
  done

  echo "RESTORE ${items} FROM Disk('backups', '${prefix}/')${settings}"
  local start
  start="$(date +%s)"
  ch_local "SET log_queries = 0; SET max_execution_time = 0; RESTORE ${items} FROM Disk('backups', '${prefix}/')${settings}"
  echo "Restore of ${prefix} finished in $(( $(date +%s) - start ))s"
  remove_download "$prefix"
  [[ -z "$base" ]] || remove_download "$base"
}

cmd="restore"
prefix=""
assume_yes=0
for arg in "$@"; do
  case "$arg" in
    list|restore|inspect) cmd="$arg" ;;
    -y|--yes) assume_yes=1 ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) prefix="${arg%/}" ;;
  esac
done

load_s3_config
ensure_local_stack

if [[ "$cmd" == "inspect" ]]; then
  inspect_prefix "${prefix:?usage: inspect <prefix>}"
  exit 0
fi

if [[ "$cmd" == "list" ]]; then
  list_prefixes
  exit 0
fi

completed="$(list_prefixes)"
if [[ -z "$prefix" ]]; then
  prefix="$(grep '^full-' <<<"$completed" | tail -n1 || true)"
  [[ -n "$prefix" ]] || { echo "No completed full-* backup found in ${S3_BUCKET}" >&2; exit 1; }
  echo "Using newest completed prod full backup: ${prefix}"
elif ! grep -qx "$prefix" <<<"$completed"; then
  echo "${prefix} is not a completed backup (still being written, failed, or missing). Completed:" >&2
  echo "$completed" >&2
  exit 1
fi

restore_prefix "$prefix" "$assume_yes"
