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

{% if is_incremental() %}
  {% set threshold_sql %}
    SELECT max(date_time) - INTERVAL 1 DAY FROM {{ this }}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 and threshold_result.rows[0][0] %}
    {% set threshold = threshold_result.rows[0][0] | string %}
  {% else %}
    {% set threshold = '1900-01-01 00:00:00' %}
  {% endif %}
  {% set since = incremental_since_var() %}
  {% if since is not none %}
    {% set threshold = since %}
  {% endif %}
{% endif %}

-- Incremental share-to-USD prices from Beefy API moo-token snapshots (already PPFS-adjusted).
-- Snapshots start in 2026. Older prices are databarn v1 ppfs * beefy wants price, with a want price at most
-- 1 day old. Ppfs comes from vault stats and from share-rate price feeds: most vaults retired before 2023 only
-- have the feed. The two agree (daily p99 gap 0.12%). Their __rmt_version is only the seconds into the slot, so a snapshot or correction always wins.
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
-- Boost receipts are not in /mootokenprices and are omitted.

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
{% if is_incremental() %}
where moo.date_time >= toDateTime('{{ threshold }}')
  and moo.date_time < now() + interval 1 day
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
  toDecimal256(toFloat64(stats.ppfs) * toFloat64(wants.price), 20) as share_token_price_usd,
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
      {% if is_incremental() %}
      and datetime >= toDateTime('{{ threshold }}')
      {% endif %}

    union all

    select assumeNotNull(s.product_id) as product_id, r.datetime as datetime, r.price as ppfs
    from {{ ref('stg_databarn_v1__prices_raw') }} r
    inner join {{ ref('stg_databarn_v1__import_states') }} s
      on r.price_feed_id = s.price_feed_id
      and s.import_type = 'product:share-rate'
    where r.price > 0
      {% if is_incremental() %}
      and r.datetime >= toDateTime('{{ threshold }}')
      {% endif %}
  )
  group by product_id, date_time
) stats
inner join {{ ref('stg_databarn_v1__products') }} p
  on stats.product_id = p.product_id
inner join {{ ref('int_chain_keys') }} c
  on {{ normalize_network_beefy_key('p.chain_key') }} = c.beefy_key
inner join {{ ref('int_product_keys') }} pk
  on c.chain_id = pk.chain_id
  and {{ evm_address("splitByChar(':', p.product_key)[4]") }} = pk.product_address
asof inner join (
  select beefy_price_oracle_key, date_time, price
  from {{ ref('int_beefy_oracle_price') }} FINAL
) wants
  on pk.beefy_wants_price_oracle_key = wants.beefy_price_oracle_key
  and stats.stats_datetime >= wants.date_time
where stats.stats_datetime < wants.date_time + interval 1 day
