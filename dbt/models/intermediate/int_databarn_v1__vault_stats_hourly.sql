{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}
{% set threshold = since %}
{% if is_incremental() and since is none %}
  {% set threshold_sql %}
    SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 and threshold_result.rows[0][0] %}
    {% set threshold = threshold_result.rows[0][0] | string %}
  {% else %}
    {% set threshold = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}

-- Last databarn v1 vault stats of each hour, valued with a want oracle price at most 1 day old.
-- Fallback for int_product_stats__tvl_hourly and int_product_stats__lps_breakdown_hourly.
-- After a databarn_v1 backfill: make dbt reimport int_databarn_v1__vault_stats_hourly+ <since>.

select
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  stats.date_hour as date_hour,
  stats.last_staked_underlying as staked_underlying,
  stats.last_underlying_total_supply as underlying_total_supply,
  toFloat64(wants.price) as want_price_usd
from (
  select
    product_id,
    toStartOfHour(datetime) as date_hour,
    max(datetime) as stats_datetime,
    toFloat64(argMax(staked_underlying, datetime)) as last_staked_underlying,
    toFloat64(argMax(underlying_total_supply, datetime)) as last_underlying_total_supply
  from {{ ref('stg_databarn_v1__vault_stats') }}
  where staked_underlying is not null
    and underlying_total_supply is not null
    {% if threshold is not none %}
    and datetime >= toDateTime('{{ threshold }}')
    {% endif %}
    {% if until is not none %}
    and datetime < toDateTime('{{ until }}')
    {% endif %}
  group by product_id, date_hour
) stats
inner join {{ ref('stg_databarn_v1__products') }} p
  on stats.product_id = p.product_id
inner join {{ ref('int_chain_keys') }} c
  on {{ normalize_network_beefy_key('p.chain_key') }} = c.beefy_key
inner join {{ ref('int_product_keys') }} pk
  on c.chain_id = pk.chain_id
  and {{ evm_address("splitByChar(':', p.product_key)[4]") }} = pk.product_address
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
