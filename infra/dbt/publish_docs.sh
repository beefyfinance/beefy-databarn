#!/bin/sh
# Generate dbt artifacts + Docglow static site and publish index.html.
# Holds the same flock as dbt run so generate cannot race a scheduler/manual run.
# Usage: publish_docs.sh

set -eu

if [ "${PUBLISH_DOCS_LOCKED:-}" != "1" ]; then
  LOCKFILE="${DBT_RUN_LOCKFILE:-/app/dbt/.dbt_run.lock}"
  LOCK_TIMEOUT="${DBT_RUN_LOCK_TIMEOUT:-30}"
  mkdir -p "$(dirname "$LOCKFILE")"
  export PUBLISH_DOCS_LOCKED=1
  if [ "$LOCK_TIMEOUT" -gt 0 ]; then
    echo "flock -x -w $LOCK_TIMEOUT $LOCKFILE $0"
    exec flock -x -w "$LOCK_TIMEOUT" "$LOCKFILE" "$0" "$@"
  else
    echo "flock -x $LOCKFILE $0"
    exec flock -x "$LOCKFILE" "$0" "$@"
  fi
fi

DBT_DIR="${DBT_PROJECT_DIR:-/app/dbt}"
PUBLISH_DIR="${DBT_DOCS_PUBLISH_DIR:-/app/dbt-docs}"
DOCGLOW_OUT="${DOCGLOW_OUT_DIR:-/tmp/docglow-site}"

cd "$DBT_DIR"
export DBT_PROFILES_DIR="$DBT_DIR"
export DBT_PROJECT_DIR="$DBT_DIR"
# Never block on Docglow's interactive telemetry consent prompt.
export DOCGLOW_NO_TELEMETRY=1

dbt_packages_dir="$DBT_DIR/dbt_packages"
if [ ! -d "$dbt_packages_dir" ] || [ -z "$(ls -A "$dbt_packages_dir" 2>/dev/null || true)" ]; then
  echo "dbt_packages empty, running 'dbt deps'..."
  uv run dbt deps
fi

echo "Generating dbt catalog/manifest..."
uv run dbt docs generate

echo "Generating Docglow static site..."
rm -rf "$DOCGLOW_OUT"
mkdir -p "$DOCGLOW_OUT"
uv run docglow generate \
  --project-dir "$DBT_DIR" \
  --output-dir "$DOCGLOW_OUT" \
  --static \
  --enable-erd

if [ -f "$DOCGLOW_OUT/index.html" ]; then
  SRC="$DOCGLOW_OUT/index.html"
elif [ -f "$DBT_DIR/target/docglow/index.html" ]; then
  SRC="$DBT_DIR/target/docglow/index.html"
else
  echo "Error: Docglow did not write index.html" >&2
  ls -la "$DOCGLOW_OUT" "$DBT_DIR/target/docglow" 2>/dev/null || true
  exit 1
fi

# Persist docglow's column-lineage cache next to manifest/catalog (target is a shared volume)
# so the MCP server can serve column lineage. DOCGLOW_OUT is wiped on every run.
LINEAGE_CACHE=".docglow-column-lineage-cache.json"
if [ -f "$DOCGLOW_OUT/$LINEAGE_CACHE" ]; then
  mkdir -p "$DBT_DIR/target/docglow"
  cp "$DOCGLOW_OUT/$LINEAGE_CACHE" "$DBT_DIR/target/docglow/$LINEAGE_CACHE.tmp"
  mv -f "$DBT_DIR/target/docglow/$LINEAGE_CACHE.tmp" "$DBT_DIR/target/docglow/$LINEAGE_CACHE"
  echo "Persisted $LINEAGE_CACHE -> $DBT_DIR/target/docglow/"
else
  echo "Warning: $DOCGLOW_OUT/$LINEAGE_CACHE not found; MCP column lineage will be unavailable" >&2
fi

mkdir -p "$PUBLISH_DIR"
TMP="$PUBLISH_DIR/index.html.tmp"
cp "$SRC" "$TMP"
mv -f "$TMP" "$PUBLISH_DIR/index.html"
echo "Published $SRC -> $PUBLISH_DIR/index.html"
