{{
  config(
    materialized='view',
    tags=['marts', 'tvl', 'stats', 'chains'],
  )
}}

SELECT
  cs.chain_id,
  cs.chain_name,
  cs.beefy_key,
  cs.beefy_enabled,
  argMax(cs.date_hour, cs.date_hour) as date_hour,
  argMax(cs.tvl_usd, cs.date_hour) as tvl_usd,
  argMax(cs.vault_tvl_usd, cs.date_hour) as vault_tvl_usd,
  argMax(cs.clm_tvl_usd, cs.date_hour) as clm_tvl_usd,
  argMax(cs.avg_beefy_performance_fee, cs.date_hour) as avg_beefy_performance_fee,
  argMax(cs.avg_lp_fee, cs.date_hour) as avg_lp_fee,
  argMax(cs.avg_compoundings_per_year, cs.date_hour) as avg_compoundings_per_year,
  argMax(cs.avg_apy, cs.date_hour) as avg_apy,
  argMax(cs.avg_total_apy, cs.date_hour) as avg_total_apy,
  argMax(cs.avg_vault_apr, cs.date_hour) as avg_vault_apr,
  argMax(cs.avg_trading_apr, cs.date_hour) as avg_trading_apr,
  argMax(cs.avg_clm_apr, cs.date_hour) as avg_clm_apr,
  argMax(cs.avg_reward_pool_apr, cs.date_hour) as avg_reward_pool_apr,
  argMax(cs.avg_reward_pool_trading_apr, cs.date_hour) as avg_reward_pool_trading_apr,
  argMax(cs.avg_vault_apy, cs.date_hour) as avg_vault_apy,
  argMax(cs.avg_liquid_staking_apr, cs.date_hour) as avg_liquid_staking_apr,
  argMax(cs.avg_composable_pool_apr, cs.date_hour) as avg_composable_pool_apr,
  argMax(cs.avg_merkl_apr, cs.date_hour) as avg_merkl_apr,
  argMax(cs.avg_linea_ignition_apr, cs.date_hour) as avg_linea_ignition_apr,
  argMax(cs.beefy_performance_fee_quantiles, cs.date_hour) as beefy_performance_fee_quantiles,
  argMax(cs.lp_fee_quantiles, cs.date_hour) as lp_fee_quantiles,
  argMax(cs.compoundings_per_year_quantiles, cs.date_hour) as compoundings_per_year_quantiles,
  argMax(cs.apy_quantiles, cs.date_hour) as apy_quantiles,
  argMax(cs.total_apy_quantiles, cs.date_hour) as total_apy_quantiles,
  argMax(cs.vault_apr_quantiles, cs.date_hour) as vault_apr_quantiles,
  argMax(cs.trading_apr_quantiles, cs.date_hour) as trading_apr_quantiles,
  argMax(cs.clm_apr_quantiles, cs.date_hour) as clm_apr_quantiles,
  argMax(cs.reward_pool_apr_quantiles, cs.date_hour) as reward_pool_apr_quantiles,
  argMax(cs.reward_pool_trading_apr_quantiles, cs.date_hour) as reward_pool_trading_apr_quantiles,
  argMax(cs.vault_apy_quantiles, cs.date_hour) as vault_apy_quantiles,
  argMax(cs.liquid_staking_apr_quantiles, cs.date_hour) as liquid_staking_apr_quantiles,
  argMax(cs.composable_pool_apr_quantiles, cs.date_hour) as composable_pool_apr_quantiles,
  argMax(cs.merkl_apr_quantiles, cs.date_hour) as merkl_apr_quantiles,
  argMax(cs.linea_ignition_apr_quantiles, cs.date_hour) as linea_ignition_apr_quantiles,
  argMax(cs.underlying_amount_compounded_usd, cs.date_hour) as underlying_amount_compounded_usd,
  argMax(cs.harvest_call_fee, cs.date_hour) as harvest_call_fee,
  argMax(cs.harvest_call_fee_usd, cs.date_hour) as harvest_call_fee_usd,
  argMax(cs.gas_fee, cs.date_hour) as gas_fee,
  argMax(cs.gas_fee_usd, cs.date_hour) as gas_fee_usd,
  argMax(cs.platform_fee, cs.date_hour) as platform_fee,
  argMax(cs.platform_fee_usd, cs.date_hour) as platform_fee_usd,
  argMax(cs.strategist_fee, cs.date_hour) as strategist_fee,
  argMax(cs.strategist_fee_usd, cs.date_hour) as strategist_fee_usd,
  argMax(cs.harvest_amount, cs.date_hour) as harvest_amount,
  argMax(cs.harvest_amount_usd, cs.date_hour) as harvest_amount_usd,
  argMax(cs.avg_native_price, cs.date_hour) as avg_native_price,
  argMax(cs.harvest_txn_count, cs.date_hour) as harvest_txn_count,
  argMax(cs.harvest_vault_count, cs.date_hour) as harvest_vault_count
FROM {{ ref('chain_stats') }} cs

-- date filter for perf, we don't expect latest stats to be more than 15 days old
WHERE cs.date_hour >= now() - INTERVAL 15 DAY

GROUP BY
  cs.chain_id,
  cs.chain_name,
  cs.beefy_key,
  cs.beefy_enabled
