{{
  config(
    materialized='table',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    engine='CoalescingMergeTree',
    on_schema_change='append_new_columns',
  )
}}

{% if is_incremental() %}
  {% set max_date_sql %}
    select max(cast(date_hour as Nullable(DateTime('UTC')))) as max_date_hour
    from {{ this }}
  {% endset %}
  {% set max_date_tbl = run_query(max_date_sql) %}
  {% set max_date = max_date_tbl.columns[0][0] %}
  {% if max_date is none %}
    {% set max_date = '2021-07-31 19:30:00' %}
  {% endif %}
{% else %}
  {% set max_date = '2021-07-31 19:30:00' %}
{% endif %}

-- Materialized intermediate: Unified hourly stats from all sources
-- Uses UNION ALL + CoalescingMergeTree instead of GROUP BY for better performance
-- CoalescingMergeTree automatically merges rows with the same sorting key,
-- retaining the latest non-NULL values for each column during background merges
-- This avoids the memory-intensive GROUP BY aggregation

-- TVL data
SELECT
  chain_id,
  product_address,
  date_hour,
  tvl_usd,
  NULL as apy,
  NULL as compoundings_per_year,
  NULL as beefy_performance_fee,
  NULL as lp_fee,
  NULL as total_apy,
  NULL as vault_apr,
  NULL as trading_apr,
  NULL as clm_apr,
  NULL as reward_pool_apr,
  NULL as reward_pool_trading_apr,
  NULL as vault_apy,
  NULL as liquid_staking_apr,
  NULL as composable_pool_apr,
  NULL as merkl_apr,
  NULL as linea_ignition_apr,
  NULL as lp_price,
  [] as breakdown_tokens,
  [] as breakdown_balances,
  NULL as total_supply,
  NULL as underlying_liquidity,
  [] as underlying_balances,
  NULL as underlying_price,
  NULL as underlying_amount_compounded,
  NULL as underlying_token_price_usd,
  NULL as underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__tvl_hourly') }}
{% if is_incremental() %}
  WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
{% endif %}

UNION ALL

-- APY data
SELECT
  chain_id,
  product_address,
  date_hour,
  NULL as tvl_usd,
  apy,
  NULL as compoundings_per_year,
  NULL as beefy_performance_fee,
  NULL as lp_fee,
  NULL as total_apy,
  NULL as vault_apr,
  NULL as trading_apr,
  NULL as clm_apr,
  NULL as reward_pool_apr,
  NULL as reward_pool_trading_apr,
  NULL as vault_apy,
  NULL as liquid_staking_apr,
  NULL as composable_pool_apr,
  NULL as merkl_apr,
  NULL as linea_ignition_apr,
  NULL as lp_price,
  [] as breakdown_tokens,
  [] as breakdown_balances,
  NULL as total_supply,
  NULL as underlying_liquidity,
  [] as underlying_balances,
  NULL as underlying_price,
  NULL as underlying_amount_compounded,
  NULL as underlying_token_price_usd,
  NULL as underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__apy_hourly') }}
{% if is_incremental() %}
  WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
{% endif %}

UNION ALL

-- APY breakdown data
SELECT
  chain_id,
  product_address,
  date_hour,
  NULL as tvl_usd,
  NULL as apy,
  compoundings_per_year,
  beefy_performance_fee,
  lp_fee,
  total_apy,
  vault_apr,
  trading_apr,
  clm_apr,
  reward_pool_apr,
  reward_pool_trading_apr,
  vault_apy,
  liquid_staking_apr,
  composable_pool_apr,
  merkl_apr,
  linea_ignition_apr,
  NULL as lp_price,
  [] as breakdown_tokens,
  [] as breakdown_balances,
  NULL as total_supply,
  NULL as underlying_liquidity,
  [] as underlying_balances,
  NULL as underlying_price,
  NULL as underlying_amount_compounded,
  NULL as underlying_token_price_usd,
  NULL as underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__apy_breakdown_hourly') }}
{% if is_incremental() %}
  WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
{% endif %}

UNION ALL

-- LPS breakdown data
SELECT
  chain_id,
  product_address,
  date_hour,
  NULL as tvl_usd,
  NULL as apy,
  NULL as compoundings_per_year,
  NULL as beefy_performance_fee,
  NULL as lp_fee,
  NULL as total_apy,
  NULL as vault_apr,
  NULL as trading_apr,
  NULL as clm_apr,
  NULL as reward_pool_apr,
  NULL as reward_pool_trading_apr,
  NULL as vault_apy,
  NULL as liquid_staking_apr,
  NULL as composable_pool_apr,
  NULL as merkl_apr,
  NULL as linea_ignition_apr,
  lp_price,
  breakdown_tokens,
  breakdown_balances,
  total_supply,
  underlying_liquidity,
  underlying_balances,
  underlying_price,
  NULL as underlying_amount_compounded,
  NULL as underlying_token_price_usd,
  NULL as underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__lps_breakdown_hourly') }}
{% if is_incremental() %}
  WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
{% endif %}

UNION ALL

-- Yield data
SELECT
  chain_id,
  product_address,
  date_hour,
  NULL as tvl_usd,
  NULL as apy,
  NULL as compoundings_per_year,
  NULL as beefy_performance_fee,
  NULL as lp_fee,
  NULL as total_apy,
  NULL as vault_apr,
  NULL as trading_apr,
  NULL as clm_apr,
  NULL as reward_pool_apr,
  NULL as reward_pool_trading_apr,
  NULL as vault_apy,
  NULL as liquid_staking_apr,
  NULL as composable_pool_apr,
  NULL as merkl_apr,
  NULL as linea_ignition_apr,
  NULL as lp_price,
  [] as breakdown_tokens,
  [] as breakdown_balances,
  NULL as total_supply,
  NULL as underlying_liquidity,
  [] as underlying_balances,
  NULL as underlying_price,
  underlying_amount_compounded,
  underlying_token_price_usd,
  underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__yield_hourly') }}
{% if is_incremental() %}
  WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
{% endif %}