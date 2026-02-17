{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}


SELECT
  p.chain_id,
  p.product_address,
  toStartOfHour(ab.date_time) as date_hour,
  argMax(ab.compoundings_per_year, ab.date_time) as compoundings_per_year,
  argMax(ab.beefy_performance_fee, ab.date_time) as beefy_performance_fee,
  argMax(ab.lp_fee, ab.date_time) as lp_fee,
  argMax(ab.total_apy, ab.date_time) as total_apy,
  argMax(ab.vault_apr, ab.date_time) as vault_apr,
  argMax(ab.trading_apr, ab.date_time) as trading_apr,
  argMax(ab.clm_apr, ab.date_time) as clm_apr,
  argMax(ab.reward_pool_apr, ab.date_time) as reward_pool_apr,
  argMax(ab.reward_pool_trading_apr, ab.date_time) as reward_pool_trading_apr,
  argMax(ab.vault_apy, ab.date_time) as vault_apy,
  argMax(ab.liquid_staking_apr, ab.date_time) as liquid_staking_apr,
  argMax(ab.composable_pool_apr, ab.date_time) as composable_pool_apr,
  argMax(ab.merkl_apr, ab.date_time) as merkl_apr,
  argMax(ab.linea_ignition_apr, ab.date_time) as linea_ignition_apr
FROM {{ ref('stg_beefy_api__apy_breakdown') }} ab
INNER JOIN {{ ref('product') }} p
  ON ab.vault_id = p.beefy_key
WHERE 
  ab.compoundings_per_year between 0 and 1000000000
  and ab.beefy_performance_fee between 0 and 1000000000
  and ab.lp_fee between 0 and 1000000000
  and ab.total_apy between 0 and 1000000
  and ab.vault_apr between 0 and 1000000
  and ab.trading_apr between 0 and 1000000
  and ab.clm_apr between 0 and 1000000
  and ab.reward_pool_apr between 0 and 1000000
  and ab.reward_pool_trading_apr between 0 and 1000000
  and ab.vault_apy between 0 and 1000000
GROUP BY p.chain_id, p.product_address, toStartOfHour(ab.date_time)