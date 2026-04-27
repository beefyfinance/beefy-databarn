{{
  config(
    materialized='view',
    tags=['marts', 'tvl', 'stats'],
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
  argMax(ps.date_hour, ps.date_hour) as date_hour,
  argMax(ps.tvl_usd, ps.date_hour) as tvl_usd,
  argMax(ps.apy, ps.date_hour) as apy,
  argMax(ps.compoundings_per_year, ps.date_hour) as compoundings_per_year,
  argMax(ps.beefy_performance_fee, ps.date_hour) as beefy_performance_fee,
  argMax(ps.lp_fee, ps.date_hour) as lp_fee,
  argMax(ps.total_apy, ps.date_hour) as total_apy,
  argMax(ps.vault_apr, ps.date_hour) as vault_apr,
  argMax(ps.trading_apr, ps.date_hour) as trading_apr,
  argMax(ps.clm_apr, ps.date_hour) as clm_apr,
  argMax(ps.reward_pool_apr, ps.date_hour) as reward_pool_apr,
  argMax(ps.reward_pool_trading_apr, ps.date_hour) as reward_pool_trading_apr,
  argMax(ps.vault_apy, ps.date_hour) as vault_apy,
  argMax(ps.liquid_staking_apr, ps.date_hour) as liquid_staking_apr,
  argMax(ps.composable_pool_apr, ps.date_hour) as composable_pool_apr,
  argMax(ps.merkl_apr, ps.date_hour) as merkl_apr,
  argMax(ps.linea_ignition_apr, ps.date_hour) as linea_ignition_apr,
  argMax(ps.lp_price, ps.date_hour) as lp_price,
  argMax(ps.breakdown_tokens, ps.date_hour) as breakdown_tokens,
  argMax(ps.breakdown_balances, ps.date_hour) as breakdown_balances,
  argMax(ps.total_supply, ps.date_hour) as total_supply,
  argMax(ps.underlying_liquidity, ps.date_hour) as underlying_liquidity,
  argMax(ps.underlying_balances, ps.date_hour) as underlying_balances,
  argMax(ps.underlying_price, ps.date_hour) as underlying_price,
  argMax(ps.underlying_amount_compounded, ps.date_hour) as underlying_amount_compounded,
  argMax(ps.underlying_token_price_usd, ps.date_hour) as underlying_token_price_usd,
  argMax(ps.underlying_amount_compounded_usd, ps.date_hour) as underlying_amount_compounded_usd
FROM {{ ref('product_stats') }} ps

-- date filter for perf, we don't expect latest stats to be more than 15 days old
WHERE ps.date_hour >= now() - INTERVAL 15 DAY

GROUP BY 
  ps.chain_id,
  ps.product_address
