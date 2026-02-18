{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'tvl', 'stats', 'chains'],
    order_by=['date_hour', 'chain_id'],
  )
}}

WITH feebatch_revenue_hourly AS (
  SELECT
    toInt64(fh.chain_id) AS chain_id,
    toStartOfHour(fh.txn_timestamp) AS date_hour,
    {{ to_decimal('sum(fh.treasury_amt)') }} AS feebatch_revenue_usd
  FROM {{ ref('stg_beefy_db__feebatch_harvests') }} fh
  GROUP BY toInt64(fh.chain_id), toStartOfHour(fh.txn_timestamp)
)

SELECT
  c.chain_id as chain_id,
  c.chain_name as chain_name,
  c.beefy_key as beefy_key,
  c.beefy_enabled as beefy_enabled,
  ps.date_hour as date_hour,
  {{ to_decimal('sum(ps.tvl_usd)') }} as tvl_usd,
  {{ to_decimal('coalesce(any(fb.feebatch_revenue_usd), 0)') }} as feebatch_revenue_usd,
  {{ to_decimal('sum(if(ps.product_type = \'classic_vault\', ps.tvl_usd, 0))') }} as vault_tvl_usd,
  {{ to_decimal('sum(if(ps.product_type = \'clm_manager\', ps.tvl_usd, 0))') }} as clm_tvl_usd,
  -- Fee averages (separate columns for easy querying)
  avg(ps.beefy_performance_fee) as avg_beefy_performance_fee,
  avg(ps.lp_fee) as avg_lp_fee,
  avg(ps.compoundings_per_year) as avg_compoundings_per_year,
  -- APR/APY averages (separate columns for easy querying)
  avg(ps.apy) as avg_apy,
  avg(ps.total_apy) as avg_total_apy,
  avg(ps.vault_apr) as avg_vault_apr,
  avg(ps.trading_apr) as avg_trading_apr,
  avg(ps.clm_apr) as avg_clm_apr,
  avg(ps.reward_pool_apr) as avg_reward_pool_apr,
  avg(ps.reward_pool_trading_apr) as avg_reward_pool_trading_apr,
  avg(ps.vault_apy) as avg_vault_apy,
  avg(ps.liquid_staking_apr) as avg_liquid_staking_apr,
  avg(ps.composable_pool_apr) as avg_composable_pool_apr,
  avg(ps.merkl_apr) as avg_merkl_apr,
  avg(ps.linea_ignition_apr) as avg_linea_ignition_apr,
  -- Fee quantiles: [min, p25, p50, p75, max] (excluding NULLs)
  {{ quantiles('ps.beefy_performance_fee') }} as beefy_performance_fee_quantiles,
  {{ quantiles('ps.lp_fee') }} as lp_fee_quantiles,
  {{ quantiles('ps.compoundings_per_year') }} as compoundings_per_year_quantiles,
  -- APR/APY quantiles: [min, p25, p50, p75, max] (excluding NULLs)
  -- Access: apy_quantiles[0] for min, apy_quantiles[1] for p25, apy_quantiles[2] for p50, etc.
  {{ quantiles('ps.apy') }} as apy_quantiles,
  {{ quantiles('ps.total_apy') }} as total_apy_quantiles,
  {{ quantiles('ps.vault_apr') }} as vault_apr_quantiles,
  {{ quantiles('ps.trading_apr') }} as trading_apr_quantiles,
  {{ quantiles('ps.clm_apr') }} as clm_apr_quantiles,
  {{ quantiles('ps.reward_pool_apr') }} as reward_pool_apr_quantiles,
  {{ quantiles('ps.reward_pool_trading_apr') }} as reward_pool_trading_apr_quantiles,
  {{ quantiles('ps.vault_apy') }} as vault_apy_quantiles,
  {{ quantiles('ps.liquid_staking_apr') }} as liquid_staking_apr_quantiles,
  {{ quantiles('ps.composable_pool_apr') }} as composable_pool_apr_quantiles,
  {{ quantiles('ps.merkl_apr') }} as merkl_apr_quantiles,
  {{ quantiles('ps.linea_ignition_apr') }} as linea_ignition_apr_quantiles,
  sum(ps.underlying_amount_compounded_usd) as underlying_amount_compounded_usd
FROM {{ ref('product_stats') }} ps
INNER JOIN {{ ref('chain') }} c
  ON ps.chain_id = c.chain_id
LEFT JOIN feebatch_revenue_hourly fb
  ON fb.chain_id = c.chain_id AND fb.date_hour = ps.date_hour
GROUP BY
  c.chain_id,
  c.chain_name,
  c.beefy_key,
  c.beefy_enabled,
  ps.date_hour
