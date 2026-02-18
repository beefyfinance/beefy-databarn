{{
  config(
    materialized='incremental',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour', 'source'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    engine='CoalescingMergeTree',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

{% if is_incremental() %}
  {% set threshold_sql %}
    SELECT
      (SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }} WHERE source = 'tvl') AS threshold_tvl,
      (SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }} WHERE source = 'apy') AS threshold_apy,
      (SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }} WHERE source = 'apy_breakdown') AS threshold_apy_breakdown,
      (SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }} WHERE source = 'lps_breakdown') AS threshold_lps_breakdown,
      (SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }} WHERE source = 'yield') AS threshold_yield
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 %}
    {% set row = threshold_result.rows[0] %}
    {% set threshold_tvl = row[0] | string %}
    {% set threshold_apy = row[1] | string %}
    {% set threshold_apy_breakdown = row[2] | string %}
    {% set threshold_lps_breakdown = row[3] | string %}
    {% set threshold_yield = row[4] | string %}
  {% else %}
    {% set threshold_tvl = '1900-01-01 00:00:00' %}
    {% set threshold_apy = '1900-01-01 00:00:00' %}
    {% set threshold_apy_breakdown = '1900-01-01 00:00:00' %}
    {% set threshold_lps_breakdown = '1900-01-01 00:00:00' %}
    {% set threshold_yield = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}

-- Materialized intermediate: Unified hourly stats from all sources
-- Incremental on date_hour with per-source max so each sub-table can progress independently.
-- One row per (chain_id, product_address, date_hour, source); unique_key dedup on incremental load.
-- Thresholds are pre-computed in one query (above) and reused in each branch.

-- TVL data (incremental: only hours we don't already have TVL for)
SELECT
  chain_id,
  product_address,
  date_hour,
  'tvl' AS source,
  tvl_usd,
  NULL AS apy,
  NULL AS compoundings_per_year,
  NULL AS beefy_performance_fee,
  NULL AS lp_fee,
  NULL AS total_apy,
  NULL AS vault_apr,
  NULL AS trading_apr,
  NULL AS clm_apr,
  NULL AS reward_pool_apr,
  NULL AS reward_pool_trading_apr,
  NULL AS vault_apy,
  NULL AS liquid_staking_apr,
  NULL AS composable_pool_apr,
  NULL AS merkl_apr,
  NULL AS linea_ignition_apr,
  NULL AS lp_price,
  [] AS breakdown_tokens,
  [] AS breakdown_balances,
  NULL AS total_supply,
  NULL AS underlying_liquidity,
  [] AS underlying_balances,
  NULL AS underlying_price,
  NULL AS underlying_amount_compounded,
  NULL AS underlying_token_price_usd,
  NULL AS underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__tvl_hourly') }}
{% if is_incremental() %}
WHERE date_hour >= toDateTime('{{ threshold_tvl }}') and date_hour < now() + INTERVAL 1 DAY
{% endif %}

UNION ALL

-- APY data (incremental: only hours we don't already have APY for)
SELECT
  chain_id,
  product_address,
  date_hour,
  'apy' AS source,
  NULL AS tvl_usd,
  apy,
  NULL AS compoundings_per_year,
  NULL AS beefy_performance_fee,
  NULL AS lp_fee,
  NULL AS total_apy,
  NULL AS vault_apr,
  NULL AS trading_apr,
  NULL AS clm_apr,
  NULL AS reward_pool_apr,
  NULL AS reward_pool_trading_apr,
  NULL AS vault_apy,
  NULL AS liquid_staking_apr,
  NULL AS composable_pool_apr,
  NULL AS merkl_apr,
  NULL AS linea_ignition_apr,
  NULL AS lp_price,
  [] AS breakdown_tokens,
  [] AS breakdown_balances,
  NULL AS total_supply,
  NULL AS underlying_liquidity,
  [] AS underlying_balances,
  NULL AS underlying_price,
  NULL AS underlying_amount_compounded,
  NULL AS underlying_token_price_usd,
  NULL AS underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__apy_hourly') }}
{% if is_incremental() %}
WHERE date_hour >= toDateTime('{{ threshold_apy }}') and date_hour < now() + INTERVAL 1 DAY
{% endif %}

UNION ALL

-- APY breakdown data (incremental: only hours we don't already have breakdown for)
SELECT
  chain_id,
  product_address,
  date_hour,
  'apy_breakdown' AS source,
  NULL AS tvl_usd,
  NULL AS apy,
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
  NULL AS lp_price,
  [] AS breakdown_tokens,
  [] AS breakdown_balances,
  NULL AS total_supply,
  NULL AS underlying_liquidity,
  [] AS underlying_balances,
  NULL AS underlying_price,
  NULL AS underlying_amount_compounded,
  NULL AS underlying_token_price_usd,
  NULL AS underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__apy_breakdown_hourly') }}
{% if is_incremental() %}
WHERE date_hour >= toDateTime('{{ threshold_apy_breakdown }}') and date_hour < now() + INTERVAL 1 DAY
{% endif %}

UNION ALL

-- LPS breakdown data (incremental: only hours we don't already have LPS for)
SELECT
  chain_id,
  product_address,
  date_hour,
  'lps_breakdown' AS source,
  NULL AS tvl_usd,
  NULL AS apy,
  NULL AS compoundings_per_year,
  NULL AS beefy_performance_fee,
  NULL AS lp_fee,
  NULL AS total_apy,
  NULL AS vault_apr,
  NULL AS trading_apr,
  NULL AS clm_apr,
  NULL AS reward_pool_apr,
  NULL AS reward_pool_trading_apr,
  NULL AS vault_apy,
  NULL AS liquid_staking_apr,
  NULL AS composable_pool_apr,
  NULL AS merkl_apr,
  NULL AS linea_ignition_apr,
  lp_price,
  breakdown_tokens,
  breakdown_balances,
  total_supply,
  underlying_liquidity,
  underlying_balances,
  underlying_price,
  NULL AS underlying_amount_compounded,
  NULL AS underlying_token_price_usd,
  NULL AS underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__lps_breakdown_hourly') }}
{% if is_incremental() %}
WHERE date_hour >= toDateTime('{{ threshold_lps_breakdown }}') and date_hour < now() + INTERVAL 1 DAY
{% endif %}

UNION ALL

-- Yield data (incremental: only hours we don't already have yield for)
SELECT
  chain_id,
  product_address,
  date_hour,
  'yield' AS source,
  NULL AS tvl_usd,
  NULL AS apy,
  NULL AS compoundings_per_year,
  NULL AS beefy_performance_fee,
  NULL AS lp_fee,
  NULL AS total_apy,
  NULL AS vault_apr,
  NULL AS trading_apr,
  NULL AS clm_apr,
  NULL AS reward_pool_apr,
  NULL AS reward_pool_trading_apr,
  NULL AS vault_apy,
  NULL AS liquid_staking_apr,
  NULL AS composable_pool_apr,
  NULL AS merkl_apr,
  NULL AS linea_ignition_apr,
  NULL AS lp_price,
  [] AS breakdown_tokens,
  [] AS breakdown_balances,
  NULL AS total_supply,
  NULL AS underlying_liquidity,
  [] AS underlying_balances,
  NULL AS underlying_price,
  underlying_amount_compounded,
  underlying_token_price_usd,
  underlying_amount_compounded_usd
FROM {{ ref('int_product_stats__yield_hourly') }}
{% if is_incremental() %}
WHERE date_hour >= toDateTime('{{ threshold_yield }}') and date_hour < now() + INTERVAL 1 DAY
{% endif %}