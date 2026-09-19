{{
  config(
    materialized='view',
    order_by=['date_time', 'vault_id'],
  )
}}

SELECT
  toInt64(t.compoundings_per_year) as compoundings_per_year,
  toFloat64(t.beefy_performance_fee) as beefy_performance_fee,
  toFloat64(t.lp_fee) as lp_fee,
  toFloat64(t.total_apy) as total_apy,
  toFloat64(t.vault_apr) as vault_apr,
  toFloat64(t.trading_apr) as trading_apr,
  toFloat64(t.clm_apr) as clm_apr,
  toFloat64(t.reward_pool_apr) as reward_pool_apr,
  toFloat64(t.reward_pool_trading_apr) as reward_pool_trading_apr,
  cast(t.etag as String) as etag,
  cast(t.vault_id as String) as vault_id,
  cast(t.date_time as DateTime('UTC')) as date_time,
  toFloat64(t.vault_apy) as vault_apy,
  toFloat64(t.liquid_staking_apr) as liquid_staking_apr,
  toFloat64(t.composable_pool_apr) as composable_pool_apr,
  toFloat64(t.merkl_apr) as merkl_apr,
  toFloat64(t.linea_ignition_apr) as linea_ignition_apr
FROM {{ source('dlt', 'beefy_api___apy_breakdown') }} t FINAL

