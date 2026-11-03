#!/usr/bin/env bash
# Restore a prod ClickHouse backup into local ClickHouse, straight from the remote S3 server prod pushes to.
#
# Runs inside the local clickhouse-backup toolbox container (mc + clickhouse-client): lists the bucket,
# then RESTOREs from S3() directly, so nothing is copied to a local object store first.
# Only the data databases are restored (not system.users etc.), so local service users/passwords stay as-is.
# Read-only on the remote bucket: nothing is pushed or pruned.
#
# Usage:
#   infra/dev/pull-prod-backup.sh list             # list prod backup prefixes
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

ch_local() {
  in_toolbox 'clickhouse-client --host "$CLICKHOUSE_BACKUP_CH_HOST" --user backup \
    --password "${CLICKHOUSE_BACKUP_PASSWORD:-$CLICKHOUSE_PASSWORD}" \
    --receive_timeout 14400 --send_timeout 14400 --multiquery --query "$1"' "$1"
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

  for db in "${dbs[@]}"; do
    ch_local "DROP DATABASE IF EXISTS \`${db}\` SYNC"
    [[ -n "$items" ]] && items+=", "
    items+="DATABASE \`${db}\`"
  done

  # An incremental's base full is resolved from its own metadata (same remote bucket and keys).
  echo "RESTORE ${items} FROM ${S3_ENDPOINT%/}/${S3_BUCKET}/${prefix}/"
  local start
  start="$(date +%s)"
  ch_local "SET log_queries = 0; RESTORE ${items} FROM S3('${S3_ENDPOINT%/}/${S3_BUCKET}/${prefix}/', '${S3_ACCESS_KEY}', '${S3_SECRET_KEY}')"
  echo "Restore of ${prefix} finished in $(( $(date +%s) - start ))s"
}

cmd="restore"
prefix=""
assume_yes=0
for arg in "$@"; do
  case "$arg" in
    list|restore) cmd="$arg" ;;
    -y|--yes) assume_yes=1 ;;
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    *) prefix="${arg%/}" ;;
  esac
done

load_s3_config
ensure_local_stack

if [[ "$cmd" == "list" ]]; then
  list_prefixes
  exit 0
fi

if [[ -z "$prefix" ]]; then
  prefix="$(list_prefixes | grep '^full-' | tail -n1)"
  [[ -n "$prefix" ]] || { echo "No full-* backup found in ${S3_BUCKET}" >&2; exit 1; }
  echo "Using newest prod full backup: ${prefix}"
fi

restore_prefix "$prefix" "$assume_yes"
