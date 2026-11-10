{{
  config(
    materialized='view',
    tags=['tvl', 'stats', 'chains'],
  )
}}

SELECT
  cs.chain_id,
  cs.chain_name,
  cs.beefy_key,
  cs.beefy_enabled,
  max(cs.date_hour) as as_of,
  argMax(cs.tvl_usd, cs.date_hour) as tvl_usd,
  argMax(cs.vault_tvl_usd, cs.date_hour) as vault_tvl_usd,
  argMax(cs.clm_tvl_usd, cs.date_hour) as clm_tvl_usd,
  argMax(cs.avg_apy, cs.date_hour) as avg_apy,
  argMax(cs.avg_apy_vault, cs.date_hour) as avg_apy_vault,
  argMax(cs.avg_apr_vault, cs.date_hour) as avg_apr_vault,
  argMax(cs.avg_apr_trading, cs.date_hour) as avg_apr_trading,
  argMax(cs.avg_apr_clm, cs.date_hour) as avg_apr_clm,
  argMax(cs.avg_apr_reward_pool, cs.date_hour) as avg_apr_reward_pool,
  argMax(cs.avg_apr_reward_pool_trading, cs.date_hour) as avg_apr_reward_pool_trading,
  argMax(cs.avg_apr_liquid_staking, cs.date_hour) as avg_apr_liquid_staking,
  argMax(cs.avg_apr_composable_pool, cs.date_hour) as avg_apr_composable_pool,
  argMax(cs.avg_apr_merkl, cs.date_hour) as avg_apr_merkl,
  argMax(cs.avg_apr_linea_ignition, cs.date_hour) as avg_apr_linea_ignition,
  argMax(cs.avg_fee_rate_performance, cs.date_hour) as avg_fee_rate_performance,
  argMax(cs.avg_fee_rate_lp, cs.date_hour) as avg_fee_rate_lp,
  argMax(cs.avg_compoundings_per_year, cs.date_hour) as avg_compoundings_per_year,
  argMax(cs.apy_quantiles, cs.date_hour) as apy_quantiles,
  argMax(cs.apy_vault_quantiles, cs.date_hour) as apy_vault_quantiles,
  argMax(cs.fee_rate_performance_quantiles, cs.date_hour) as fee_rate_performance_quantiles,
  argMax(cs.fee_rate_lp_quantiles, cs.date_hour) as fee_rate_lp_quantiles,
  argMax(cs.compoundings_per_year_quantiles, cs.date_hour) as compoundings_per_year_quantiles,
  argMax(cs.harvest_count, cs.date_hour) as harvest_count,
  argMax(cs.harvest_product_count, cs.date_hour) as harvest_product_count,
  argMax(cs.harvest_amount, cs.date_hour) as harvest_amount,
  argMax(cs.harvest_amount_usd, cs.date_hour) as harvest_amount_usd,
  argMax(cs.harvest_call_fee, cs.date_hour) as harvest_call_fee,
  argMax(cs.harvest_call_fee_usd, cs.date_hour) as harvest_call_fee_usd,
  argMax(cs.harvest_gas_fee, cs.date_hour) as harvest_gas_fee,
  argMax(cs.harvest_gas_fee_usd, cs.date_hour) as harvest_gas_fee_usd,
  argMax(cs.harvest_platform_fee, cs.date_hour) as harvest_platform_fee,
  argMax(cs.harvest_platform_fee_usd, cs.date_hour) as harvest_platform_fee_usd,
  argMax(cs.harvest_strategist_fee, cs.date_hour) as harvest_strategist_fee,
  argMax(cs.harvest_strategist_fee_usd, cs.date_hour) as harvest_strategist_fee_usd,
  argMax(cs.harvest_native_price_usd, cs.date_hour) as harvest_native_price_usd
FROM {{ ref('fact_chain_stats_1h') }} cs

-- date filter for perf, we don't expect latest stats to be more than 15 days old
WHERE cs.date_hour >= now() - INTERVAL 15 DAY

GROUP BY
  cs.chain_id,
  cs.chain_name,
  cs.beefy_key,
  cs.beefy_enabled
