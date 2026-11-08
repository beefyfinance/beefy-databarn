-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    post_hook="{{ price_consumer_done('int_databarn_v1__vault_stats_hourly') }}",
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
{% set rp = price_reprice('int_databarn_v1__vault_stats_hourly', threshold, 'asset', 'hour') if is_incremental() else {'head_from': none, 'keys': []} %}
{% set want_asset = asset_id_from_beefy_price_oracle_id('pk.beefy_wants_price_oracle_id') %}

-- Last databarn v1 vault stats of each hour, valued with a want oracle price at most 1 day old.
-- Fallback for int_product_stats__tvl_hourly and int_product_stats__lps_breakdown_hourly.
-- After a databarn_v1 backfill: make dbt reimport int_databarn_v1__vault_stats_hourly+ <since>.
-- Incremental runs also recompute the hours a resolver run re-priced (price_reprice: each dirty want from the hour
-- of its first changed price).

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
    and (
      datetime >= toDateTime('{{ threshold }}')
      -- products whose want price was re-priced (the want join below keeps the re-priced hours only)
      or product_id in (
        select p.product_id
        from {{ ref('stg_databarn_v1__products') }} p
        inner join {{ ref('int_chain_keys') }} c on {{ normalize_network_beefy_key('p.chain_key') }} = c.beefy_key
        inner join {{ ref('int_product_keys') }} pk
          on c.chain_id = pk.chain_id and {{ evm_address("splitByChar(':', p.product_key)[4]") }} = pk.product_address
        where {{ price_repriced(rp, want_asset, "toDateTime(4294967295, 'UTC')") }}
      )
    )
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
inner join (
  -- The wants only, in the window: asset_price_5m is dense, so the want price at the last stats row of the hour is
  -- the row of its 5-minute slot (an oracle price is carried at most 1 day).
  select asset_id, date_time, price
  from {{ ref('asset_price_5m') }}
  where asset_id in (
      select {{ asset_id_from_beefy_price_oracle_id('beefy_wants_price_oracle_id') }} from {{ ref('int_product_keys') }}
      where beefy_wants_price_oracle_id is not null
    )
    {% if threshold is not none %}
    and (
      date_time >= toStartOfFiveMinutes(toDateTime('{{ threshold }}'))
      or {{ price_repriced(rp, 'asset_id', 'date_time') }}
    )
    {% endif %}
    {% if until is not none %}
    and date_time < toDateTime('{{ until }}')
    {% endif %}
) wants
  on wants.asset_id = {{ want_asset }}
  and wants.date_time = toStartOfFiveMinutes(stats.stats_datetime)
