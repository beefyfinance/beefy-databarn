{{
  config(
    materialized='incremental',
    tags=['intermediate'],
    order_by=['chain_id', 'product_address', 'date_time'],
    engine='ReplacingMergeTree(__rmt_version)',
    partition_by='toYYYYMM(date_time)',
    on_schema_change='append_new_columns',
    incremental_strategy='append',
  )
}}

{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}
{% set threshold = since %}
{% if is_incremental() and since is none %}
  {% set threshold_sql %}
    SELECT max(date_time) - INTERVAL 1 DAY FROM {{ this }}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 and threshold_result.rows[0][0] %}
    {% set threshold = threshold_result.rows[0][0] | string %}
  {% else %}
    {% set threshold = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}

-- Incremental share-to-USD prices from Beefy API moo-token snapshots (already PPFS-adjusted).
-- share_token_price_usd is the USD value of 1e18 raw share units, which is one token: shares report 18 decimals.
-- Shares are minted 1:1 with want units, so a non-18-decimal want gives a price far from the want price
-- (a USDC vault is about 1e12 * ppfs). stg_beefy_api__mootokenprices rescales older snapshots to this unit.
-- Snapshots start in 2026. Older prices are databarn v1 ppfs * beefy wants price, with a want price at most
-- 1 day old. Ppfs comes from vault stats and from share-rate price feeds: most vaults retired before 2023 only
-- have the feed. The two agree (daily p99 gap 0.12%). Their __rmt_version is only the seconds into the slot, so a snapshot or correction always wins.
-- CLM managers and reward pools are not in /mootokenprices: their shares are priced by a beefy_db oracle, hourly.
-- The CLM subgraph is the fallback for hours with no oracle price (live p50 gap to the oracle 0.15%).
-- After a databarn_v1 backfill: make dbt reimport int_product_share_price+ <since>.
-- Grain is a 5-minute slot: beefy_api dlt snapshots run every 5 minutes (infra/dlt/scheduler.py)
-- and are bucketed with toStartOfFiveMinutes.
-- Raw snapshots and int_mootoken_price_corrections rows are a plain union all. ReplacingMergeTree
-- keeps the highest __rmt_version per (chain_id, product_address, date_time). Read with FINAL.
-- Append only (no delete step): __rmt_version = (run time * 2 + is_correction) * 300 + seconds into the slot,
-- so the latest run wins; within a run corrections beat snapshots and the latest snapshot of the slot wins.
-- After removing a window, make dbt reimport int_product_share_price+ <since> re-inserts the raw snapshots
-- with a newer version.
-- All corrected rows are always re-emitted. Incremental runs load new snapshots with a 1-day lookback.
-- Boost receipts are not here: token maps them to their vault's asset.

select
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  pk.beefy_key as beefy_key,
  toStartOfFiveMinutes(moo.date_time) as date_time,
  cast({{ to_decimal('moo.price') }} as Decimal(76, 20)) as share_token_price_usd,
  cast('api' as String) as price_source,
  (toUInt64(toUnixTimestamp(now())) * 2 + 0) * 300
    + toUInt64(toUnixTimestamp(moo.date_time) - toUnixTimestamp(toStartOfFiveMinutes(moo.date_time))) as __rmt_version
from {{ ref('stg_beefy_api__mootokenprices') }} moo
inner join {{ ref('int_chain_keys') }} c
  on {{ normalize_network_beefy_key('moo.chain_id') }} = c.beefy_key
inner join {{ ref('int_product_keys') }} pk
  on c.chain_id = pk.chain_id
  and moo.moo_token_symbol = pk.share_token_symbol
{% if threshold is not none %}
where moo.date_time >= toDateTime('{{ threshold }}')
  and moo.date_time < {% if until is not none %}toDateTime('{{ until }}'){% else %}now() + interval 1 day{% endif %}
{% endif %}

union all

-- Same run, is_correction = 1: outranks every snapshot of the run.
select
  corr.chain_id as chain_id,
  corr.product_address as product_address,
  corr.beefy_key as beefy_key,
  corr.date_time as date_time,
  corr.share_token_price_usd as share_token_price_usd,
  corr.price_source as price_source,
  (toUInt64(toUnixTimestamp(now())) * 2 + 1) * 300 as __rmt_version
from {{ ref('int_mootoken_price_corrections') }} corr

union all

-- databarn v1 ppfs: lowest priority, whatever the run.
select
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  pk.beefy_key as beefy_key,
  stats.date_time as date_time,
  -- databarn v1 ppfs is want tokens per 1e18 raw share units, the unit of this column: no decimals scaling.
  toDecimal256(toFloat64(assumeNotNull(stats.ppfs)) * toFloat64(wants.price), 20) as share_token_price_usd,
  cast('databarn_v1' as String) as price_source,
  toUInt64(stats.seconds_into_slot) as __rmt_version
from (
  select
    product_id,
    toStartOfFiveMinutes(datetime) as date_time,
    max(datetime) as stats_datetime,
    argMax(ppfs, datetime) as ppfs,
    max(toUnixTimestamp(datetime) - toUnixTimestamp(toStartOfFiveMinutes(datetime))) as seconds_into_slot
  from (
    select product_id, datetime, share_to_underlying_price as ppfs
    from {{ ref('stg_databarn_v1__vault_stats') }}
    where share_to_underlying_price > 0
      {% if threshold is not none %}
      and datetime >= toDateTime('{{ threshold }}')
      {% endif %}
      {% if until is not none %}
      and datetime < toDateTime('{{ until }}')
      {% endif %}

    union all

    select assumeNotNull(s.product_id) as product_id, r.datetime as datetime, r.price as ppfs
    from {{ ref('stg_databarn_v1__prices_raw') }} r
    inner join {{ ref('stg_databarn_v1__import_states') }} s
      on r.price_feed_id = s.price_feed_id
      and s.import_type = 'product:share-rate'
    where r.price > 0
      {% if threshold is not none %}
      and r.datetime >= toDateTime('{{ threshold }}')
      {% endif %}
      {% if until is not none %}
      and r.datetime < toDateTime('{{ until }}')
      {% endif %}
  )
  group by product_id, date_time
) stats
inner join {{ ref('int_databarn_v1__want_decimals') }} wd
  on stats.product_id = wd.product_id
inner join {{ ref('int_product_keys') }} pk
  on wd.chain_id = pk.chain_id
  and wd.product_address = pk.product_address
asof inner join (
  -- The whole table does not fit in the asof hash table (30 GiB): only the wants, from 1 day before the window.
  select beefy_price_oracle_key, date_time, price
  from {{ ref('int_beefy_oracle_price') }} FINAL
  where beefy_price_oracle_key in (select beefy_wants_price_oracle_key from {{ ref('int_product_keys') }})
    {% if threshold is not none %}
    and date_time >= toDateTime64('{{ threshold }}', 0) - interval 1 day
    {% endif %}
    {% if until is not none %}
    and date_time < toDateTime('{{ until }}')
    {% endif %}
) wants
  on pk.beefy_wants_price_oracle_key = wants.beefy_price_oracle_key
  and stats.stats_datetime >= wants.date_time
where stats.stats_datetime < wants.date_time + interval 1 day

union all

-- beefy_db oracle of CLM managers and reward pools: hourly, last price of the hour. Outranks the CLM subgraph.
select
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  pk.beefy_key as beefy_key,
  o.date_hour as date_time,
  cast(o.price as Decimal(76, 20)) as share_token_price_usd,
  cast('beefy_db' as String) as price_source,
  toUInt64(1) as __rmt_version
from (
  select
    beefy_price_oracle_key,
    toStartOfHour(date_time) as date_hour,
    argMax(price, date_time) as price
  from {{ ref('int_beefy_oracle_price') }} FINAL
  where beefy_price_oracle_key in (
    select beefy_share_price_oracle_key from {{ ref('int_product_keys') }} where beefy_share_price_oracle_key is not null
  )
    {% if threshold is not none %}
    and date_time >= toDateTime('{{ threshold }}')
    {% endif %}
    {% if until is not none %}
    and date_time < toDateTime('{{ until }}')
    {% endif %}
  group by beefy_price_oracle_key, date_hour
) o
inner join {{ ref('int_product_keys') }} pk
  on o.beefy_price_oracle_key = pk.beefy_share_price_oracle_key

union all

-- CLM subgraph: lowest priority, hourly.
select
  c.chain_id as chain_id,
  c.product_address as product_address,
  pk.beefy_key as beefy_key,
  c.date_hour as date_time,
  toDecimal256(assumeNotNull(c.share_token_price_usd), 20) as share_token_price_usd,
  cast('clm_subgraph' as String) as price_source,
  toUInt64(0) as __rmt_version
from {{ ref('int_beefy_clm_subgraph__clm_stats_hourly') }} c
inner join {{ ref('int_product_keys') }} pk
  on c.chain_id = pk.chain_id
  and c.product_address = pk.product_address
where c.share_token_price_usd > 0
  {% if threshold is not none %}
  and c.date_hour >= toDateTime('{{ threshold }}')
  {% endif %}
  {% if until is not none %}
  and c.date_hour < toDateTime('{{ until }}')
  {% endif %}
