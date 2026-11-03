#!/usr/bin/env bash
# Pull a prod ClickHouse backup (from prod RustFS) into local RustFS and RESTORE it into local ClickHouse.
#
# Prod RustFS is only on the prod docker network, so we SSH-tunnel to the rustfs container IP,
# `mc mirror` the backup prefix into the local RustFS bucket, then RESTORE from the local copy.
# Only the data databases are restored (not system.users etc.), so local service users/passwords stay as-is.
#
# Usage:
#   infra/dev/pull-prod-backup.sh list             # list prod backup prefixes
#   infra/dev/pull-prod-backup.sh [<prefix>] [-y]  # pull + restore (default: newest full-*)
#   infra/dev/pull-prod-backup.sh pull [<prefix>]  # pull only, no restore
#
# Env (from .env):
#   PROD_SSH_HOST   ssh target for the prod host, e.g. databarn@databarn.example.com (required)
#   PROD_REPO_DIR   repo checkout on the prod host (default: ~/beefy-databarn)
#   PROD_TUNNEL_PORT local port for the tunnel (default: 19102)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

PROD_SSH_HOST="${PROD_SSH_HOST:?PROD_SSH_HOST must be set (e.g. databarn@prod-host)}"
PROD_REPO_DIR="${PROD_REPO_DIR:-~/beefy-databarn}"
TUNNEL_PORT="${PROD_TUNNEL_PORT:-19102}"
LOCAL_BUCKET="${CLICKHOUSE_BACKUP_S3_BUCKET:-clickhouse-backups}"
LOCAL_S3_ACCESS_KEY="${CLICKHOUSE_BACKUP_S3_ACCESS_KEY:-${RUSTFS_ACCESS_KEY:-admin}}"
LOCAL_S3_SECRET_KEY="${CLICKHOUSE_BACKUP_S3_SECRET_KEY:-${RUSTFS_SECRET_KEY:?RUSTFS_SECRET_KEY must be set}}"
LOCAL_NETWORK="databarn_network"
MC_IMAGE="minio/mc:latest"
DC=(docker compose -f "$ROOT_DIR/infra/dev/docker-compose.yml" --env-file "$ROOT_DIR/.env")
PROD_DC="docker compose -f ${PROD_REPO_DIR}/infra/prod/docker-compose.yml --env-file ${PROD_REPO_DIR}/.env"

TUNNEL_PID=""
cleanup() {
  if [[ -n "$TUNNEL_PID" ]]; then
    kill "$TUNNEL_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

prod_ssh() {
  ssh -o BatchMode=yes "$PROD_SSH_HOST" "$@"
}

open_tunnel() {
  local rustfs_id rustfs_ip
  echo "Locating prod rustfs container on ${PROD_SSH_HOST}..."
  rustfs_id="$(prod_ssh "${PROD_DC} ps -q rustfs")"
  [[ -n "$rustfs_id" ]] || { echo "prod rustfs container not running" >&2; exit 1; }
  rustfs_ip="$(prod_ssh "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' ${rustfs_id}" | awk '{print $1}')"
  [[ -n "$rustfs_ip" ]] || { echo "could not resolve prod rustfs IP" >&2; exit 1; }

  # Read prod S3 creds from the backup sidecar's env (never echoed).
  PROD_S3_ACCESS_KEY="$(prod_ssh "${PROD_DC} exec -T clickhouse-backup printenv CLICKHOUSE_BACKUP_S3_ACCESS_KEY")"
  PROD_S3_SECRET_KEY="$(prod_ssh "${PROD_DC} exec -T clickhouse-backup printenv CLICKHOUSE_BACKUP_S3_SECRET_KEY")"
  PROD_BUCKET="$(prod_ssh "${PROD_DC} exec -T clickhouse-backup printenv CLICKHOUSE_BACKUP_S3_BUCKET")"

  echo "Opening tunnel localhost:${TUNNEL_PORT} -> ${rustfs_ip}:9000"
  ssh -o BatchMode=yes -o ExitOnForwardFailure=yes -N -L "${TUNNEL_PORT}:${rustfs_ip}:9000" "$PROD_SSH_HOST" &
  TUNNEL_PID=$!
  local i
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:${TUNNEL_PORT}/health" >/dev/null 2>&1; then
      return 0
    fi
    kill -0 "$TUNNEL_PID" 2>/dev/null || { echo "ssh tunnel exited" >&2; exit 1; }
    sleep 1
  done
  echo "tunnel to prod rustfs not healthy" >&2
  exit 1
}

# Run mc with `prod` (via the tunnel) and `local` (dev rustfs on the compose network) aliases.
mc_run() {
  docker run --rm -i \
    --network "$LOCAL_NETWORK" \
    --add-host host.docker.internal:host-gateway \
    -e PROD_S3_ACCESS_KEY="$PROD_S3_ACCESS_KEY" -e PROD_S3_SECRET_KEY="$PROD_S3_SECRET_KEY" \
    -e LOCAL_S3_ACCESS_KEY="$LOCAL_S3_ACCESS_KEY" -e LOCAL_S3_SECRET_KEY="$LOCAL_S3_SECRET_KEY" \
    --entrypoint /bin/sh "$MC_IMAGE" -c '
      mc alias set prod "http://host.docker.internal:'"$TUNNEL_PORT"'" "$PROD_S3_ACCESS_KEY" "$PROD_S3_SECRET_KEY" >/dev/null &&
      mc alias set local http://rustfs:9000 "$LOCAL_S3_ACCESS_KEY" "$LOCAL_S3_SECRET_KEY" >/dev/null &&
      mc "$@"' mc "$@"
}

list_prod_prefixes() {
  mc_run ls "prod/${PROD_BUCKET}/" | awk '{print $NF}' | sed 's:/$::' | grep -E '^(full|inc)-' | sort
}

ensure_local_stack() {
  if ! docker network inspect "$LOCAL_NETWORK" >/dev/null 2>&1; then
    echo "Local stack not running (no ${LOCAL_NETWORK}); run: make infra start" >&2
    exit 1
  fi
  "${DC[@]}" up -d rustfs clickhouse clickhouse-backup >/dev/null
  "${DC[@]}" run --rm rustfs-init >/dev/null
}

pull_prefix() {
  local prefix="$1"
  echo "Mirroring prod ${PROD_BUCKET}/${prefix} -> local ${LOCAL_BUCKET}/${prefix}"
  mc_run mirror --overwrite --remove "prod/${PROD_BUCKET}/${prefix}" "local/${LOCAL_BUCKET}/${prefix}"
}

backup_databases() {
  local prefix="$1"
  mc_run ls "local/${LOCAL_BUCKET}/${prefix}/metadata/" | awk '{print $NF}' | grep -E '\.sql$' | sed 's/\.sql$//' \
    | grep -vE '^(system|INFORMATION_SCHEMA|information_schema)$' || true
}

ch_local() {
  "${DC[@]}" exec -T clickhouse-backup bash -c 'clickhouse-client --host "$CLICKHOUSE_BACKUP_CH_HOST" --user backup \
    --password "${CLICKHOUSE_BACKUP_PASSWORD:-$CLICKHOUSE_PASSWORD}" \
    --receive_timeout 14400 --send_timeout 14400 --multiquery --query "$1"' _ "$1"
}

restore_prefix() {
  local prefix="$1" assume_yes="$2"
  local -a dbs=()
  local db items=""
  while IFS= read -r db; do
    [[ -n "$db" ]] && dbs+=("$db")
  done < <(backup_databases "$prefix")
  ((${#dbs[@]} > 0)) || { echo "No databases found in ${prefix}/metadata" >&2; exit 1; }

  echo "Will DROP and restore local databases: ${dbs[*]}"
  if [[ "$assume_yes" != "1" ]]; then
    read -r -p "Continue? [y/N] " answer
    [[ "$answer" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
  fi

  for db in "${dbs[@]}"; do
    ch_local "DROP DATABASE IF EXISTS \`${db}\` SYNC"
    [[ -n "$items" ]] && items+=", "
    items+="DATABASE \`${db}\`"
  done

  echo "RESTORE ${items} FROM local ${LOCAL_BUCKET}/${prefix}"
  local start
  start="$(date +%s)"
  ch_local "SET log_queries = 0; RESTORE ${items} FROM S3('http://rustfs:9000/${LOCAL_BUCKET}/${prefix}/', '${LOCAL_S3_ACCESS_KEY}', '${LOCAL_S3_SECRET_KEY}')"
  echo "Restore of ${prefix} finished in $(( $(date +%s) - start ))s"
}

cmd="restore"
prefix=""
assume_yes=0
for arg in "$@"; do
  case "$arg" in
    list|pull|restore) cmd="$arg" ;;
    -y|--yes) assume_yes=1 ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) prefix="${arg%/}" ;;
  esac
done

ensure_local_stack
open_tunnel

if [[ "$cmd" == "list" ]]; then
  list_prod_prefixes
  exit 0
fi

if [[ -z "$prefix" ]]; then
  prefix="$(list_prod_prefixes | grep '^full-' | tail -n1)"
  [[ -n "$prefix" ]] || { echo "No full-* backup found on prod" >&2; exit 1; }
  echo "Using newest prod full backup: ${prefix}"
fi

# An incremental needs its base full (same day) present next to it.
if [[ "$prefix" == inc-* ]]; then
  base="full-$(echo "$prefix" | sed -E 's/^inc-([0-9]{4}-[0-9]{2}-[0-9]{2})-.*/\1/')"
  pull_prefix "$base"
fi
pull_prefix "$prefix"

if [[ "$cmd" == "restore" ]]; then
  restore_prefix "$prefix" "$assume_yes"
fi
