# beefy-history ingest

Catalog lifecycle lives in git, not in `api.beefy.finance`. Databarn does **not** parse git and does **not** put DuckDB on the warehouse path.

```text
git mirrors of beefy-v2 + beefy-app
  → beefy-history CLI (core package) → parquet
  → upload to RustFS bucket `beefy-history/` (never clickhouse-backups)
  → ClickHouse s3() of current/*.parquet
  → dbt staging tables (MergeTree copy)
  → int_beefy_history__* lifecycle facts
```

## Pinned CLI

| | |
|---|---|
| Repo | https://github.com/mrt-bots/beefy-history |
| SHA | `79dd2fcc18d0373798eeeec9b9726dd876c39b89` (`dlt/lib/beefy_history.py` `CLI_GIT_SHA` and `infra/dlt/Dockerfile`) |
| Engine | `beefy-history/4+engine/5+parse/3` |
| Invoke | `node packages/core/src/cli.ts <command>` from the clone (Node 24 type-stripping) |

Bump the SHA in both places when the parquet contract or engine version changes, then rebuild the dlt image.

## CLI

```sh
# first run clones both remotes (~450 MB) into --repos-dir, then walks first-parent prod
node packages/core/src/cli.ts sync --out data/store --repos-dir repos

# later runs fetch + incremental append. Identity/engine version mismatch → rebuild into a fresh --out.
node packages/core/src/cli.ts sync --out data/store --repos-dir repos --no-fetch   # mirrors only, no git fetch
node packages/core/src/cli.ts status --out data/store --repos-dir repos
node packages/core/src/cli.ts compact --out data/store
node packages/core/src/cli.ts query "SELECT count(*) FROM events" --out data/store
```

Existing clones (read-only, never fetched):

```sh
node packages/core/src/cli.ts sync --app-dir ../beefy-app --v2-dir ../beefy-v2 --v2-ref origin/prod --out data/store
```

Flags: `--out`, `--repos-dir` (default `repos/beefy-app.git` + `repos/beefy-v2.git` bare mirrors), `--app-url` / `--v2-url`, `--app-branch` / `--v2-branch` (default `prod`), `--app-dir` / `--v2-dir`, `--app-ref` / `--v2-ref`, `--no-fetch`, `--flush-every` (default 250), `--quiet`, `--json` (query).

Databarn’s producer always **rebuilds** `--out` (full walk is ~30 s once the mirrors exist; ~7k objects / ~56k events) then `compact` so each table is one parquet file.

## Store files (on disk)

```
<data>/repos/beefy-app.git
<data>/repos/beefy-v2.git
<data>/store/manifest.json          commit point (only these files are live)
<data>/store/events/events-<gen>.parquet | base-<gen>.parquet
<data>/store/issues/issues-<gen>.parquet | base-<gen>.parquet
<data>/store/state/state-<gen>.json.zst   engine checkpoint (not published)
```

`objects` and `latest` are DuckDB **views** over `events`, not parquet. Do not expect `objects*.parquet`.

### Nested `data` / `changes`

- `data`: `VARCHAR` of canonical JSON (sorted keys). `NULL` on `type = 'removed'`. ClickHouse: `JSONExtract*`.
- `changedKeys`: `VARCHAR[]` / parquet LIST of top-level keys that changed (`NULL` unless `type = 'changed'`).
- Event column `type` is the change (`added` / `changed` / `removed` / `readded`). Config `type` (`standard` / `gov` / `cowcentrated` / `erc4626`) is inside `data.type`. Fallback: `data.isGovVault` → `gov`, else `standard`.

## RustFS

Bucket **`beefy-history`** (created by `rustfs-init`). Atomic publish:

1. `run=<id>/events.parquet`, `issues.parquet`, `manifest.json`
2. overwrite `current/` (dbt may only glob `current/`)

```sql
SELECT * FROM s3(beefy_history_s3_events)   -- named collection → current/events.parquet
```

## Run it

```sh
# prod / dlt container (Node + pinned CLI are in the image)
make dlt run beefy_history

# host: Node 24, BEEFY_HISTORY_DIR pointing at a clone at CLI_GIT_SHA, RustFS up
uv run --env-file .env ./beefy_history_pipeline.py   # from dlt/
```

Scheduled hourly at :15 UTC from `infra/dlt/scheduler.py` (next to the dlt pipelines, not an HTTP dlt source). First clone of the two Beefy repos is the slow step; keep `BEEFY_HISTORY_DATA_DIR` on disk.

Env: `BEEFY_HISTORY_DIR`, `BEEFY_HISTORY_DATA_DIR` (default `$STORAGE_DIR/beefy-history` or `/var/beefy-history`), `BEEFY_HISTORY_S3_ENDPOINT` (container `http://rustfs:9000`, host `$RUSTFS_ENDPOINT`), `BEEFY_HISTORY_S3_BUCKET`, keys fall back to `CLICKHOUSE_BACKUP_S3_*` / `RUSTFS_*`.

## dbt

Two marts. Filter / group at query time — there are no chart-shaped tables.

| Model | Grain | What it is |
|---|---|---|
| `stg_beefy_history__events` | `seq` | Copy of `current/events.parquet` + flattened config columns |
| `stg_beefy_history__issues` | `seq` | Copy of `current/issues.parquet` |
| `stg_beefy_history__objects` | `object_id` (`kind:chain:address`) | Latest snapshot per contract, derived from events |
| `int_beefy_history__event_windows` | `(object_id, seq)` | In-catalog + observed status from this event until the next |
| `int_beefy_history__vault_lifecycle` | `object_id` (vaults) | `first_active_at`, `last_active_end_at`, retirement, CLM parent |
| `beefy_history_objects` | `object_id` | Current-state + launch / retire / platform / lifespan |
| `beefy_history_events` | `seq` | Catalog changes with valid windows, `prev_data`, commit fields |

Identity is **chain + contract address**, not `beefy_key`. Boosts are ingested if present; `/stats` ignores them (`WHERE counts_for_stats`). Run this producer once before `dbt run` or the `s3()` copy has nothing to read.

### Recreating [history.beefy.rodeo](https://history.beefy.rodeo)

Same predicates as the app: vaults only on `/stats`; empty/missing status = active; CLM wrappers excluded (`counts_for_stats`); launch = first in-catalog active; retirement = end of last active period if not active now and not paused. Month length is 2_629_800 s (30.4375 days).

| Page | Table | Filter |
|---|---|---|
| `/` search + facets | objects | `kind`, `chain`, `vault_type`, `status`, `live`, `name`, ids, addresses |
| `/inactive` | objects | `WHERE is_inactive` |
| `/o/[objectId]` | both | objects row + events `WHERE object_id = … ORDER BY seq` (`prev_data` for diffs) |
| `/changes` | events | `ORDER BY seq DESC`; filter `change_type`, `reason`, `kind`, `chain`, `changed_keys`, `committed_at` |
| `/commits/[repo]/[sha]` | events | `WHERE commit_repo AND commit_sha`; index = `GROUP BY commit_repo, commit_sha` |
| `/stats` (all but the time series) | objects | `WHERE counts_for_stats` then `GROUP BY` below |
| `/stats` active-over-time | events | windows: `counts_for_stats AND is_active AND valid_from_unix <= T AND (valid_to_unix IS NULL OR T < valid_to_unix)` |

```sql
-- /stats tiles (peak is the events query at many T; take max)
SELECT
  countIf(is_currently_active) AS active_now,
  uniqExactIf(chain, is_currently_active) AS chains_now,
  uniqExact(chain) AS chains_ever,
  count() AS launched,
  countIf(is_retired) AS retired,
  quantileExact(0.5)(lifespan_months) AS median_lifespan_months
FROM beefy_history_objects
WHERE counts_for_stats;

-- launches by type (or chain)
SELECT launch_quarter, vault_type, count()
FROM beefy_history_objects
WHERE counts_for_stats
GROUP BY launch_quarter, vault_type;

-- retirements by reason group
SELECT retirement_quarter, retire_reason_group, count()
FROM beefy_history_objects
WHERE counts_for_stats AND is_retired
GROUP BY retirement_quarter, retire_reason_group;

-- platforms
SELECT stats_platform, count() AS launched, countIf(is_currently_active) AS active_now
FROM beefy_history_objects
WHERE counts_for_stats
GROUP BY stats_platform;

-- lifespan histogram
SELECT lifespan_months, count()
FROM beefy_history_objects
WHERE counts_for_stats AND is_retired
GROUP BY lifespan_months;

-- active counted vaults at unix T (generate Mondays + month-starts + now in the client)
SELECT chain, vault_type, count()
FROM beefy_history_events
WHERE counts_for_stats AND is_active
  AND valid_from_unix <= T
  AND (valid_to_unix IS NULL OR T < valid_to_unix)
GROUP BY chain, vault_type;

-- commit index
SELECT
  commit_repo, commit_sha, any(commit_subject), min(committed_at),
  count() AS event_count, uniqExact(object_id) AS object_count
FROM beefy_history_events
GROUP BY commit_repo, commit_sha;
```
