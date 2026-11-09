# Model and database conventions

How models, folders and ClickHouse databases are organised. `macros/generate_alias_name.sql` enforces the prefixes and
the grain suffixes. Until its readers have moved, the old `analytics` database keeps views with the old table names
over the new tables: the legacy layer (section 1).

## Why

The layout before November 2026, which this replaced:

- `models/marts/` held two kinds of model. Some are reusable facts (`product_stats`, `asset_price_5m`, `harvests`).
  Others were pre-built answers to one question (`daily_revenue_summary`, `product_tvl_diff`).
- Everything a user could query sat in the one `analytics` database: dimensions, facts, marts, the API view and the
  data quality models (`api/` and `data_quality/` have no `+schema`, so they fall back to `target.schema`).
- Grain was spelled four ways: `asset_price_1d` (suffix), `daily_revenue_summary` (prefix), `int_*_hourly` (word),
  `product_stats` and `chain_stats` (not stated, but hourly).
- Two facts had the same grain: `harvests` and `yield_harvest_events` both held one row per harvest.
- Dead references: the Grafana `revenue.json` dashboard queried `analytics.revenue_summary`, which no model built. The
  README described `marts/revenue/` and `marts/yield/` folders that did not exist.

## 1. Layers

Each layer has one folder and one model prefix. The table is named like the model, prefix included. Everything users
query sits in one database, `databarn`, where the prefix sorts the tables into dimensions, facts and marts; the API
views have their own database.

| layer | what goes in it | folder | model name | database.table | audience |
|---|---|---|---|---|---|
| staging | 1:1 cleanup of a source table | `staging/<source>/` | `stg_<source>__<table>` | `dbt.stg_…` (unchanged) | internal |
| intermediate | building blocks | `intermediate/<domain>/` | `int_<domain>__<what>[_<grain>]` | `dbt.int_…` (unchanged) | internal |
| dimension | one row per entity, current state | `dimensions/` | `dim_<entity>` | `databarn.dim_<entity>` | users |
| fact | one row per observation, or per entity per period. Reusable | `facts/<domain>/` | `fact_<subject>_<grain>` | `databarn.fact_<subject>_<grain>` | users |
| mart | shaped for one consumer or question | `marts/<consumer>/` | `mart_<subject>_<grain>` | `databarn.mart_<subject>_<grain>` | users |
| api | REST API contracts | `api/` | `api_<endpoint>` | `api.<endpoint>` | API |
| legacy | temporary: old `analytics` names over the new tables | `legacy/` | `legacy_<old name>` | `analytics.<old name>` | old readers |
| data quality | lists of known problems to inspect | `data_quality/` | `dq_<check>` | `dbt.dq_<check>` | operators |

Why one database: a client lists one database at a time, so a database per layer meant switching back and forth to
find a table, while prefixes in one list sort the same way and keep everything in view
(`databarn.fact_product_stats_1h JOIN databarn.dim_product`). The table name is the model name, so a table is found
in the project by its name. Only `api` (and the temporary `legacy`) live in a database of their own and drop the
prefix there (`api_revenue_summary` is `api.revenue_summary`, section 5).

The `dbt` database holds everything users should not browse: `stg_`, `int_` and `dq_` keep their prefix there, so
one database still sorts into three groups.

### Legacy layer (temporary)

`models/legacy/` keeps the names of the old `analytics` database answering while its readers move: one view per old
table, over its new model, with renamed columns mapped back. They are dbt models so they cannot be forgotten: each
carries `deprecation_date` (dbt warns on every run after it), and `legacy_proxy_guard()` stops a proxy from replacing
an old name that is still a table. Nothing may read them in this project. To retire them: delete the folder and the
`legacy` entries (`dbt_project.yml`, `generate_alias_name.sql`, `mcp/layers.py`, `docglow.yml`), then drop the
`analytics` database.

### Data quality model or test?

- A check whose result should be empty is a **singular test** in `tests/` (like `tests/models_are_fresh.sql`). With
  `store_failures`, its failing rows land in `dbt_test__audit`.
- A list that is expected to hold rows and that someone inspects (known gaps, known mismatches) is a **`dq_` model**.
  `missing_products_in_envio` and `databarn_v1_want_decimals_mismatch` are both of this kind.
- Neither is a user-facing table, so neither has its own database.

### Fact or mart?

A model is a **fact** when all of these hold:
- each row is a real event (a harvest, a zap, a balance change) or one entity in one period (an asset in a 5-minute
  slot, a product in an hour);
- it keeps the keys to its dimensions (`chain_id`, `product_address`, `asset_id`, …) so it can be joined and sliced
  any way;
- it keeps the full history and every entity: no rolling window, no top N, no filter for one dashboard.

A model is a **mart** when any of these hold:
- it is limited to a window (`daily_revenue_summary` keeps 30 days);
- it combines unrelated facts into one shape for one use (revenue + yield + buybacks per day);
- it compares points in time (`product_tvl_diff`), pivots, or ranks;
- you would name it after the dashboard or question it answers.

A rollup of a fact along a dimension (`chain_stats` = `product_stats` summed by chain) is still a fact: it keeps full
history, its keys and a stated grain, and other models reuse it.

### Dependency rules

- `dim_` and `fact_` read `stg_`, `int_`, `dim_` and other `fact_` models. They never read `mart_`, `api_` or `dq_` models.
- `mart_` reads `dim_`, `fact_` and other `mart_` models. It does not read `stg`: if a mart needs a source,
  the logic belongs in a fact or an intermediate model. (`daily_revenue_summary` reads three staging models today.)
- `api_` reads `mart_` (or `fact_`) models only. Nothing reads `api`, `dq_` or `legacy_` models.

These rules can be checked with a small test over the manifest, or with `dbt-project-evaluator`.

## 2. Grain

**Every fact and mart ends with its grain.** A fact or mart name without a grain suffix is an error, and the alias
macro rejects it. Intermediate models follow the same suffixes, except lookup tables with no time in them
(`int_product_keys`, `int_price_series_map`). Dimensions have none: they are one row per entity by definition.

| suffix | one row per | time column |
|---|---|---|
| `_obs` | observation: one source record, not aggregated (a harvest, a zap, a balance change, a price reading) | on-chain: `block_timestamp` (+ `block_number`, `txn_hash`, `log_index`); off-chain: `ts` |
| `_5m` | entity × 5-minute slot | `date_time` |
| `_1h` | entity × hour | `date_hour` |
| `_1d` | entity × UTC day | `date_day` |
| `_1w`, `_1mo` | entity × week (Monday), month | `date_week`, `date_month` |
| `_latest` | entity, now | `as_of` |

- Grain is always a suffix, never a prefix or a word: `revenue_summary_1d`, not `daily_revenue_summary`;
  `int_product_stats__tvl_1h`, not `int_product_stats__tvl_hourly`. Intermediate and staging models follow it too.
- The time column name gives the grain on its own, so a column copied into another query keeps its meaning.
- `as_of` (`DateTime`, UTC) is the time the row's values hold for: the start of the bucket, or the timestamp of the
  observation, it was taken from. Every `_latest` table has it, whatever the grain of its source, so "how fresh is
  this" is always `now() - as_of`. The yml names the source table. When a value can be older than its bucket (a price
  carried forward), the table also keeps `observed_at`.
- Subjects are singular, like the other grains: `harvest_obs`, `zap_obs`, `asset_price_5m`.
- `_obs` is about grain, not about cleanliness: a fact `_obs` table is cleaned, keyed to dimensions and priced. It is
  "raw" only in that nothing is aggregated. Merging the legs of one record (a zap's token transfers into one row per
  source transaction) keeps it an observation.
- Two kinds of observation share the suffix, and the yml says which. An event (harvest, zap, balance change) can be
  counted and its amounts summed. A reading of a continuous value (a price) cannot: timestamps are irregular, sources
  can disagree, and it must be resolved into a bucketed table before use (`int_price_obs` → `fact_asset_price_5m`).
  Readings stay intermediate.
- Column suffixes are trailing windows or offsets, not grain: `revenue_usd_30d`, `tvl_usd_1d_ago`. Keep that
  meaning: never use `_1d` on a column to mean "daily value".

Columns (smaller, can come later): events today use `txn_timestamp` and `block_timestamp`, `txn_hash` and `trx_hash`,
`event_idx` and `log_index`. Pick one of each (`block_timestamp`, `txn_hash`, `log_index`) and rename everywhere.
`date_time` in `asset_price_latest`, `date_hour` in `product_stats_latest` and `chain_stats_latest`, and `ref_hour` in
`product_tvl_diff` all become `as_of`.

## 3. Rename map (November 2026)

### Dimensions → `databarn.dim_*`

| today | model | table |
|---|---|---|
| `analytics.account` | `dim_account` | `databarn.dim_account` |
| `analytics.asset` | `dim_asset` | `databarn.dim_asset` |
| `analytics.chain` | `dim_chain` | `databarn.dim_chain` |
| `analytics.platform` | `dim_platform` | `databarn.dim_platform` |
| `analytics.product` | `dim_product` | `databarn.dim_product` |
| `analytics.product_bundle` | `dim_product_bundle` | `databarn.dim_product_bundle` |
| `analytics.product_classic` | `dim_product_classic` | `databarn.dim_product_classic` |
| `analytics.product_classic_boost` | `dim_product_classic_boost` | `databarn.dim_product_classic_boost` |
| `analytics.product_clm` | `dim_product_clm` | `databarn.dim_product_clm` |
| `analytics.product_reward_pool` | `dim_product_reward_pool` | `databarn.dim_product_reward_pool` |
| `analytics.token` | `dim_token` | `databarn.dim_token` |

### Facts → `databarn.fact_*`

| today | model | table | grain |
|---|---|---|---|
| `asset_price_5m` | `facts/prices/fact_asset_price_5m` | `databarn.fact_asset_price_5m` | asset × 5m |
| `asset_price_1d` | `facts/prices/fact_asset_price_1d` | `databarn.fact_asset_price_1d` | asset × day |
| `asset_price_latest` | `facts/prices/fact_asset_price_latest` | `databarn.fact_asset_price_latest` | asset |
| `product_stats` | `facts/products/fact_product_stats_1h` | `databarn.fact_product_stats_1h` | product × hour |
| `product_stats_latest` | `facts/products/fact_product_stats_latest` | `databarn.fact_product_stats_latest` | product |
| `chain_stats` | `facts/chains/fact_chain_stats_1h` | `databarn.fact_chain_stats_1h` | chain × hour |
| `chain_stats_latest` | `facts/chains/fact_chain_stats_latest` | `databarn.fact_chain_stats_latest` | chain |
| `harvests` + `yield_harvest_events` | `facts/products/fact_harvest_obs` | `databarn.fact_harvest_obs` | harvest |
| `revenue_buyback_events` | `facts/revenue/fact_buyback_obs` | `databarn.fact_buyback_obs` | buyback |
| `zap_activity` | `facts/zaps/fact_zap_obs` | `databarn.fact_zap_obs` | source transaction |
| `account_balance_changes` | `facts/accounts/fact_account_balance_change_obs` | `databarn.fact_account_balance_change_obs` | balance change |
| `account_balance_daily` | `facts/accounts/fact_account_balance_1d` | `databarn.fact_account_balance_1d` | account × token × day |

`harvests` and `yield_harvest_events` held the same rows. `fact_harvest_obs` replaces both and adds `want_price`; the
yield names map to `harvest_amount`, `want_price` and `harvest_amount_usd`. `fact_fee_batch_obs` is new (fee batch
harvests, from `stg_beefy_db__feebatch_harvests`).

### Marts → `databarn.mart_*`

| today | model | table |
|---|---|---|
| `daily_revenue_summary` | `marts/revenue/mart_revenue_summary_1d` | `databarn.mart_revenue_summary_1d` |
| `product_tvl_diff` | `marts/products/mart_product_tvl_change_latest` | `databarn.mart_product_tvl_change_latest` |

`mart_revenue_summary_1d` reads `fact_buyback_obs`, `fact_harvest_obs` and `fact_fee_batch_obs` instead of the staging
models. Its buybacks are now priced like `fact_buyback_obs` (the resolved BIFI price of the 5-minute slot, else the
stored one) and skip the rows that fact filters out: the whole history differs by 0.01%, the last 7 days not at all.

### API, data quality

| today | model | table |
|---|---|---|
| `analytics.api_revenue_summary` | `api_revenue_summary` | `api.revenue_summary` |
| `analytics.databarn_v1_want_decimals_mismatch` | `dq_databarn_v1_want_decimals_mismatch` | `dbt.dq_databarn_v1_want_decimals_mismatch` |
| `analytics.missing_products_in_envio` | `dq_missing_products_in_envio` | `dbt.dq_missing_products_in_envio` |

### Intermediate (grain suffix only)

| today | new |
|---|---|
| `int_product_stats__{apy,apy_breakdown,harvest,lps_breakdown,share_price,tvl,unified,yield}_hourly` | `int_product_stats__…_1h` |
| `int_beefy_clm_subgraph__{clm_stats,share_price}_hourly` | `…_1h` |
| `int_databarn_v1__vault_stats_hourly` | `int_databarn_v1__vault_stats_1h` |

Intermediate models sit in sub-folders by domain (`prices/`, `product_stats/`, `keys/`, `databarn_v1/`,
`beefy_clm_subgraph/`), which changes no table name.

## 4. Resulting layout

```
dbt/models/
  staging/<source>/stg_<source>__<table>.sql                  → dbt.stg_*
  intermediate/<domain>/int_<domain>__<what>[_<grain>].sql     → dbt.int_*
  dimensions/dim_<entity>.sql                                  → databarn.dim_<entity>
  facts/
    prices/    fact_asset_price_{5m,1d,latest}.sql              → databarn.fact_asset_price_*
    products/  fact_product_stats_{1h,latest}.sql, fact_harvest_obs.sql
    chains/    fact_chain_stats_{1h,latest}.sql
    accounts/  fact_account_balance_change_obs.sql, fact_account_balance_1d.sql
    revenue/   fact_buyback_obs.sql, fact_fee_batch_obs.sql
    zaps/      fact_zap_obs.sql
  marts/
    revenue/   mart_revenue_summary_1d.sql                      → databarn.mart_revenue_summary_1d
    products/  mart_product_tvl_change_latest.sql
  api/api_revenue_summary.sql                                  → api.revenue_summary
  data_quality/dq_*.sql                                        → dbt.dq_*
```

ClickHouse, as a client lists it:

```
databarn  dim_account dim_asset dim_chain dim_platform dim_product dim_product_* dim_token
          fact_account_balance_1d fact_account_balance_change_obs fact_asset_price_1d fact_asset_price_5m
          fact_asset_price_latest fact_buyback_obs fact_chain_stats_1h fact_chain_stats_latest fact_fee_batch_obs
          fact_harvest_obs fact_product_stats_1h fact_product_stats_latest fact_zap_obs
          mart_product_tvl_change_latest mart_revenue_summary_1d
api       revenue_summary
dbt       stg_* int_* dq_*   (internal and operators)
dlt       raw loads          (internal)
```

## 5. Where this lives

- `dbt_project.yml`: one `+schema` per folder. `staging`, `intermediate` and `data_quality` go to the dbt database
  (`DBT_CLICKHOUSE_STAGING_DB`), dimensions, facts and marts to `databarn`, api to `api`, legacy to `analytics`.
- `macros/generate_alias_name.sql`: strips the prefix in `api` and `analytics` only, and fails the build on a
  model without its layer prefix or, for facts and marts, without its grain.
- `infra/clickhouse/init-clickhouse.sh`: creates the databases and grants them.
- `mcp/layers.py`, `docglow.yml`: the layers as the MCP server and the docs show them.

## Open questions

- Do facts carry dimension names (`product_stats_1h` has `chain_name`, `display_name`, `platform_id`, …)? Keys only is
  cleaner and avoids stale names in incremental tables; names are convenient in Grafana. Possible rule: facts carry
  keys only, `_latest` views and marts may join names.
- Event columns: `txn_timestamp` / `block_timestamp`, `txn_hash` / `trx_hash`, `event_idx` / `log_index` (section 2).
