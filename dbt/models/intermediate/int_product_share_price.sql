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
-- For now the share price comes only from /mootokenprices. Later an indexer will provide PPFS and
-- the share price becomes ppfs * beefy wants price.
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
  cast({{ to_decimal('moo.price') }} as Decimal(76, 20)) as share_to_usd,
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
  corr.share_to_usd as share_to_usd,
  corr.price_source as price_source,
  (toUInt64(toUnixTimestamp(now())) * 2 + 1) * 300 as __rmt_version
from {{ ref('int_mootoken_price_corrections') }} corr
