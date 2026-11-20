#!/bin/sh
# Run a command holding an exclusive per-pipeline file lock, so only one process at a time
# works in a dlt pipeline's working directory (/var/dlt/pipelines/<name>).
# Two processes sharing it race on pending load packages: dlt loads pending packages first, so a
# scheduled run picks up a package a manual `--loop` just normalized and both move its job files.
#
# Usage: run_with_lock.sh <pipeline_name> <command> [args...]
#   e.g. run_with_lock.sh databarn_v1 uv run ./databarn_v1_pipeline.py
#
# The lock is an flock on an fd inherited by the command (exec, no wrapper process left), so it
# is released when the command and its children exit, however they die. A killed scheduler run
# whose child lives on still holds the lock, which is right: that child is still running.
#
# DLT_RUN_LOCK_DIR      lock directory (default /var/dlt/locks, the dlt volume)
# DLT_RUN_LOCK_TIMEOUT  seconds to wait when the lock is held (default 30); 0 = wait forever
# Exits with LOCK_BUSY_EXIT_CODE when the wait times out, so callers can tell it from a
# pipeline failure. Keep in sync with LOCK_BUSY_EXIT_CODE in infra/dlt/process.py.

LOCK_BUSY_EXIT_CODE=75

if [ "$#" -lt 2 ]; then
  echo "usage: run_with_lock.sh <pipeline_name> <command> [args...]" >&2
  exit 2
fi

NAME="$1"
shift
LOCK_DIR="${DLT_RUN_LOCK_DIR:-/var/dlt/locks}"
LOCK_TIMEOUT="${DLT_RUN_LOCK_TIMEOUT:-30}"

mkdir -p "$LOCK_DIR" || exit 1
exec 9>"$LOCK_DIR/$NAME.lock" || exit 1

if ! flock -n 9; then
  echo "[run_with_lock] another dlt process holds the $NAME lock; waiting (timeout ${LOCK_TIMEOUT}s, 0 = forever)" >&2
  if [ "$LOCK_TIMEOUT" -gt 0 ]; then
    flock -w "$LOCK_TIMEOUT" -E "$LOCK_BUSY_EXIT_CODE" 9 || exit $?
  else
    flock 9 || exit $?
  fi
fi

exec "$@"
