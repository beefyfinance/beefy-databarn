{{
  config(
    materialized='view',
    tags=['tvl', 'stats'],
  )
}}

SELECT
  ps.chain_id,
  ps.product_address,
  argMax(ps.product_type, ps.date_hour) as product_type,
  argMax(ps.beefy_key, ps.date_hour) as beefy_key,
  argMax(ps.display_name, ps.date_hour) as display_name,
  argMax(ps.platform_id, ps.date_hour) as platform_id,
  argMax(ps.chain_name, ps.date_hour) as chain_name,
  argMax(ps.is_active, ps.date_hour) as is_active,
  max(ps.date_hour) as as_of,
  argMax(ps.tvl_usd, ps.date_hour) as tvl_usd,
  argMax(ps.apy, ps.date_hour) as apy,
  argMax(ps.share_price_usd, ps.date_hour) as share_price_usd,
  argMax(ps.want_price_usd, ps.date_hour) as want_price_usd,
  argMax(ps.apy_vault, ps.date_hour) as apy_vault,
  argMax(ps.apr_vault, ps.date_hour) as apr_vault,
  argMax(ps.apr_trading, ps.date_hour) as apr_trading,
  argMax(ps.apr_clm, ps.date_hour) as apr_clm,
  argMax(ps.apr_reward_pool, ps.date_hour) as apr_reward_pool,
  argMax(ps.apr_reward_pool_trading, ps.date_hour) as apr_reward_pool_trading,
  argMax(ps.apr_liquid_staking, ps.date_hour) as apr_liquid_staking,
  argMax(ps.apr_composable_pool, ps.date_hour) as apr_composable_pool,
  argMax(ps.apr_merkl, ps.date_hour) as apr_merkl,
  argMax(ps.apr_linea_ignition, ps.date_hour) as apr_linea_ignition,
  argMax(ps.fee_rate_performance, ps.date_hour) as fee_rate_performance,
  argMax(ps.fee_rate_lp, ps.date_hour) as fee_rate_lp,
  argMax(ps.compoundings_per_year, ps.date_hour) as compoundings_per_year,
  argMax(ps.want_total_supply, ps.date_hour) as want_total_supply,
  argMax(ps.want_tokens, ps.date_hour) as want_tokens,
  argMax(ps.want_balances, ps.date_hour) as want_balances,
  argMax(ps.clm_pool_liquidity, ps.date_hour) as clm_pool_liquidity,
  argMax(ps.clm_pool_liquidity_price_usd, ps.date_hour) as clm_pool_liquidity_price_usd,
  argMax(ps.clm_pool_balances, ps.date_hour) as clm_pool_balances,
  argMax(ps.harvest_count, ps.date_hour) as harvest_count,
  argMax(ps.harvest_amount, ps.date_hour) as harvest_amount,
  argMax(ps.harvest_amount_usd, ps.date_hour) as harvest_amount_usd,
  argMax(ps.harvest_call_fee, ps.date_hour) as harvest_call_fee,
  argMax(ps.harvest_call_fee_usd, ps.date_hour) as harvest_call_fee_usd,
  argMax(ps.harvest_gas_fee, ps.date_hour) as harvest_gas_fee,
  argMax(ps.harvest_gas_fee_usd, ps.date_hour) as harvest_gas_fee_usd,
  argMax(ps.harvest_platform_fee, ps.date_hour) as harvest_platform_fee,
  argMax(ps.harvest_platform_fee_usd, ps.date_hour) as harvest_platform_fee_usd,
  argMax(ps.harvest_strategist_fee, ps.date_hour) as harvest_strategist_fee,
  argMax(ps.harvest_strategist_fee_usd, ps.date_hour) as harvest_strategist_fee_usd,
  argMax(ps.harvest_native_price_usd, ps.date_hour) as harvest_native_price_usd
FROM {{ ref('fact_product_stats_1h') }} ps

-- date filter for perf, we don't expect latest stats to be more than 15 days old
WHERE ps.date_hour >= now() - INTERVAL 15 DAY

GROUP BY 
  ps.chain_id,
  ps.product_address
