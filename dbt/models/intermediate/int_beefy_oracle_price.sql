{{
  config(
    materialized='incremental',
    tags=['intermediate'],
    order_by=['beefy_price_oracle_key', 'date_time'],
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

-- USD price of Beefy price oracles over time (beefy_price_oracle_key = price_oracles.oracle_id:
-- a token symbol like 'frxETH', or an LP oracle id). Oracle prices, NOT vault share prices
-- (see int_product_share_price for those).
-- Source: beefy_db prices (full history, fixed upstream and reloaded by make reimport-prices),
-- bucketed to 5-minute slots, latest price of the slot.
-- int_price_oracle_corrections rows override the raw price on the same slot: plain union all, and
-- ReplacingMergeTree keeps the highest __rmt_version. Read with FINAL and filter on
-- beefy_price_oracle_key (primary key prefix).
-- Append only (no delete step): __rmt_version = (run time * 2 + is_correction) * 300 + seconds into the slot,
-- so the latest run wins; within a run corrections beat raw prices and the latest price of the slot wins.
-- Corrections are re-emitted every run, so they keep winning. After removing a window,
-- make dbt reimport int_beefy_oracle_price+ <since> re-inserts the raw prices with a newer version.
-- '-rp' oracles are duplicates of their base oracle and are left out.

select
  o.oracle_id as beefy_price_oracle_key,
  toStartOfFiveMinutes(p.t) as date_time,
  cast(p.val as Decimal(76, 20)) as price,
  cast('beefy_db' as String) as price_source,
  (toUInt64(toUnixTimestamp(now())) * 2 + 0) * 300
    + toUInt64(toUnixTimestamp(p.t) - toUnixTimestamp(toStartOfFiveMinutes(p.t))) as __rmt_version
from {{ ref('stg_beefy_db__prices') }} p
inner join {{ ref('stg_beefy_db__price_oracles') }} o
  on p.oracle_id = o.id
where not endsWith(o.oracle_id, '-rp')
  {% if is_incremental() %}
  and p.t >= toDateTime('{{ threshold }}')
  and p.t < now() + interval 1 day
  {% endif %}

union all

-- Same run, is_correction = 1: outranks every raw price of the run.
select
  beefy_price_oracle_key,
  date_time,
  price,
  price_source,
  (toUInt64(toUnixTimestamp(now())) * 2 + 1) * 300 as __rmt_version
from {{ ref('int_price_oracle_corrections') }}
where not endsWith(beefy_price_oracle_key, '-rp')
