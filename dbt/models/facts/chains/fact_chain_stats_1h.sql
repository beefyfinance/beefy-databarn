{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['tvl', 'stats', 'chains'],
    order_by=['date_hour', 'chain_id'],
  )
}}

SELECT
  c.chain_id as chain_id,
  c.chain_name as chain_name,
  c.beefy_key as beefy_key,
  c.beefy_enabled as beefy_enabled,
  ps.date_hour as date_hour,
  sum(ps.tvl_usd) as tvl_usd,
  sum(if(ps.product_type = 'classic_vault', ps.tvl_usd, 0)) as vault_tvl_usd,
  sum(if(ps.product_type = 'clm_manager', ps.tvl_usd, 0)) as clm_tvl_usd,
  -- Averages over the products that have the value (NULLs skipped)
  avg(ps.apy) as avg_apy,
  avg(ps.apy_vault) as avg_apy_vault,
  avg(ps.apr_vault) as avg_apr_vault,
  avg(ps.apr_trading) as avg_apr_trading,
  avg(ps.apr_clm) as avg_apr_clm,
  avg(ps.apr_reward_pool) as avg_apr_reward_pool,
  avg(ps.apr_reward_pool_trading) as avg_apr_reward_pool_trading,
  avg(ps.apr_liquid_staking) as avg_apr_liquid_staking,
  avg(ps.apr_composable_pool) as avg_apr_composable_pool,
  avg(ps.apr_merkl) as avg_apr_merkl,
  avg(ps.apr_linea_ignition) as avg_apr_linea_ignition,
  avg(ps.fee_rate_performance) as avg_fee_rate_performance,
  avg(ps.fee_rate_lp) as avg_fee_rate_lp,
  avg(ps.compoundings_per_year) as avg_compoundings_per_year,
  -- Quantiles: [min, p25, p50, p75, max] (NULLs excluded). apy_quantiles[1] is the min (arrays are 1-based).
  {{ quantiles('ps.apy') }} as apy_quantiles,
  {{ quantiles('ps.apy_vault') }} as apy_vault_quantiles,
  {{ quantiles('ps.fee_rate_performance') }} as fee_rate_performance_quantiles,
  {{ quantiles('ps.fee_rate_lp') }} as fee_rate_lp_quantiles,
  {{ quantiles('ps.compoundings_per_year') }} as compoundings_per_year_quantiles,
  -- Harvests of the hour, summed over the chain's products
  sum(ps.harvest_count) as harvest_count,
  count(ps.harvest_count) as harvest_product_count,
  sum(ps.harvest_amount) as harvest_amount,
  sum(ps.harvest_amount_usd) as harvest_amount_usd,
  sum(ps.harvest_call_fee) as harvest_call_fee,
  sum(ps.harvest_call_fee_usd) as harvest_call_fee_usd,
  sum(ps.harvest_gas_fee) as harvest_gas_fee,
  sum(ps.harvest_gas_fee_usd) as harvest_gas_fee_usd,
  sum(ps.harvest_platform_fee) as harvest_platform_fee,
  sum(ps.harvest_platform_fee_usd) as harvest_platform_fee_usd,
  sum(ps.harvest_strategist_fee) as harvest_strategist_fee,
  sum(ps.harvest_strategist_fee_usd) as harvest_strategist_fee_usd,
  avg(ps.harvest_native_price_usd) as harvest_native_price_usd
FROM {{ ref('fact_product_stats_1h') }} ps
INNER JOIN {{ ref('dim_chain') }} c
  ON ps.chain_id = c.chain_id
GROUP BY
  c.chain_id,
  c.chain_name,
  c.beefy_key,
  c.beefy_enabled,
  ps.date_hour
