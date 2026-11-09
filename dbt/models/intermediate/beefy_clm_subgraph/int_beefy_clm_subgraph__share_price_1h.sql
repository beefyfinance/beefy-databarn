{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'prices'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['chain_id', 'product_address', 'date_hour'],
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

-- Hourly share price (USD per 1e18 raw share units) of every CLM manager and classic vault the CLM subgraph
-- indexes, listed by the Beefy API or not. Unlike int_beefy_clm_subgraph__clm_stats_1h it does not join
-- int_product_keys, so a product's history exists from its deployment, before the API lists it (often days later),
-- and for products the API never lists.
-- CLM manager: same pricing as int_beefy_clm_subgraph__clm_stats_1h (both balances priced through the exact pool
-- price, in the better priced token). Live p50 gap to the CLM oracle 0.15%.
-- Classic vault (all subgraph classics stake a CLM share): ppfs = underlying_amount / vault_shares_total_supply
-- (both 18 decimals) times the hourly price of its underlying CLM manager computed above. Gap to /mootokenprices,
-- October 2026: p50 0.2% to 0.4% and p90 1% to 3% on Robinhood, Sonic, Monad and Arc; Avalanche and Rootstock are off
-- because their CLM prices are (p50 4% and 31%). The subgraph's own underlying_to_native_price is not used: it
-- truncates in native decimals (p90 58% off on Sonic, 1e12 off on Arc). A classic has no price for an hour without
-- a CLM price, so it waits for the CLM snapshot backfill. Fallback only.
-- Consumer: int_price_obs (source of fact_asset_price_5m: a fallback series of the product's share asset and of a CLM's
-- oracle asset, used after an hour with no other value).

{% set max_share_price_usd = '1e30' %}

with clm_snapshots as (
  select
    chain_dim.chain_id as chain_id,
    s.clm_address as product_address,
    s.period_start_at as date_hour,
    toFloat64(assumeNotNull(s.total_underlying_amount0)) / pow(10, t0.decimals) as balance0,
    toFloat64(assumeNotNull(s.total_underlying_amount1)) / pow(10, t1.decimals) as balance1,
    toFloat64(assumeNotNull(s.manager_total_supply)) / 1e18 as manager_total_supply,
    toFloat64(assumeNotNull(s.price_of_token0_in_token1)) / pow(10, t1.decimals) as token1_per_token0,
    toFloat64(assumeNotNull(s.token0_to_native_price)) * toFloat64(assumeNotNull(s.native_to_usd_price)) / 1e36 as token0_price_usd,
    toFloat64(assumeNotNull(s.token1_to_native_price)) * toFloat64(assumeNotNull(s.native_to_usd_price)) / 1e36 as token1_price_usd
  from {{ ref('stg_beefy_clm_subgraph__clm_snapshots') }} s
  inner join {{ ref('stg_beefy_clm_subgraph__clms') }} c
    on s.chain_key = c.chain_key
    and s.clm_address = c.clm_address
  inner join {{ ref('stg_beefy_clm_subgraph__tokens') }} t0
    on c.chain_key = t0.chain_key
    and c.underlying_token0_address = t0.token_address
  inner join {{ ref('stg_beefy_clm_subgraph__tokens') }} t1
    on c.chain_key = t1.chain_key
    and c.underlying_token1_address = t1.token_address
  inner join {{ ref('dim_chain') }} chain_dim
    on s.chain_key = chain_dim.beefy_key
  where s.period_seconds = 3600
    and s.manager_total_supply > 0
    and s.total_underlying_amount0 >= 0
    and s.total_underlying_amount1 >= 0
    {% if threshold is not none %}
    and s.period_start_at >= toDateTime('{{ threshold }}')
    {% endif %}
    {% if until is not none %}
    and s.period_start_at < toDateTime('{{ until }}')
    {% endif %}
),

clm_prices as (
  select
    chain_id,
    product_address,
    date_hour,
    if(
      token0_price_usd >= token1_price_usd,
      (balance0 + balance1 / token1_per_token0) * token0_price_usd,
      (balance0 * token1_per_token0 + balance1) * token1_price_usd
    ) / manager_total_supply as share_token_price_usd
  from clm_snapshots
  where token1_per_token0 > 0
    and greatest(token0_price_usd, token1_price_usd) > 0
),

classic_prices as (
  select
    chain_dim.chain_id as chain_id,
    s.classic_address as product_address,
    s.period_start_at as date_hour,
    toFloat64(assumeNotNull(s.underlying_amount)) / toFloat64(assumeNotNull(s.vault_shares_total_supply))
      * clm.share_token_price_usd as share_token_price_usd
  from {{ ref('stg_beefy_clm_subgraph__classic_snapshots') }} s
  inner join {{ ref('stg_beefy_clm_subgraph__classics') }} c
    on s.chain_key = c.chain_key
    and s.classic_address = c.classic_address
  inner join {{ ref('dim_chain') }} chain_dim
    on s.chain_key = chain_dim.beefy_key
  inner join clm_prices clm
    on chain_dim.chain_id = clm.chain_id
    and c.underlying_token_address = clm.product_address
    and s.period_start_at = clm.date_hour
  where s.period_seconds = 3600
    and s.vault_shares_total_supply > 0
    and s.underlying_amount > 0
    {% if threshold is not none %}
    and s.period_start_at >= toDateTime('{{ threshold }}')
    {% endif %}
    {% if until is not none %}
    and s.period_start_at < toDateTime('{{ until }}')
    {% endif %}
),

unioned as (
  select chain_id, product_address, 'clm_manager' as product_type, date_hour, share_token_price_usd, 'clm_subgraph' as price_source
  from clm_prices
  union all
  select chain_id, product_address, 'classic_vault' as product_type, date_hour, share_token_price_usd, 'classic_subgraph' as price_source
  from classic_prices
)

select
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(product_address) as product_address,
  cast(product_type as LowCardinality(String)) as product_type,
  assumeNotNull(date_hour) as date_hour,
  assumeNotNull(share_token_price_usd) as share_token_price_usd,
  cast(price_source as LowCardinality(String)) as price_source
from unioned
-- Above the maximum is a dust supply or a broken token price, and toDecimal256 would overflow downstream.
where product_address is not null
  and date_hour is not null
  and share_token_price_usd > 0
  and share_token_price_usd < {{ max_share_price_usd }}
