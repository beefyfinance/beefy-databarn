{{
  config(
    materialized='view',
  )
}}

-- Amounts and prices are raw on-chain integers: apply token decimals downstream.
SELECT
  t.chain_key AS chain_key,
  {{ evm_address('t.id') }} AS clm_address,
  t.lifecycle AS lifecycle,
  {{ evm_address('t.manager') }} AS manager_address,
  {{ evm_address('t.strategy') }} AS strategy_address,
  {{ evm_address('t.underlying_protocol_pool') }} AS underlying_protocol_pool_address,
  {{ evm_address('t.manager_token') }} AS manager_token_address,
  {{ evm_address('t.underlying_token0') }} AS underlying_token0_address,
  {{ evm_address('t.underlying_token1') }} AS underlying_token1_address,
  {{ to_str_list('t.output_tokens_order') }} AS output_tokens_order,
  {{ to_str_list('t.reward_tokens_order') }} AS reward_tokens_order,
  {{ to_str_list('t.reward_pool_tokens_order') }} AS reward_pool_tokens_order,
  toInt256(t.manager_total_supply) AS manager_total_supply,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_pools_total_supply') }}) AS reward_pools_total_supply,
  toInt256(t.token0_to_native_price) AS token0_to_native_price,
  toInt256(t.token1_to_native_price) AS token1_to_native_price,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.output_to_native_prices') }}) AS output_to_native_prices,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_to_native_prices') }}) AS reward_to_native_prices,
  toInt256(t.native_to_usd_price) AS native_to_usd_price,
  toInt256(t.price_of_token0_in_token1) AS price_of_token0_in_token1,
  toInt256(t.price_range_min1) AS price_range_min1,
  toInt256(t.price_range_max1) AS price_range_max1,
  toInt256(t.total_underlying_amount0) AS total_underlying_amount0,
  toInt256(t.total_underlying_amount1) AS total_underlying_amount1,
  toInt256(t.underlying_main_amount0) AS underlying_main_amount0,
  toInt256(t.underlying_main_amount1) AS underlying_main_amount1,
  toInt256(t.underlying_alt_amount0) AS underlying_alt_amount0,
  toInt256(t.underlying_alt_amount1) AS underlying_alt_amount1,
  toInt256(t.total_call_fees) AS total_call_fees,
  toInt256(t.total_beefy_fees) AS total_beefy_fees,
  toInt256(t.total_strategist_fees) AS total_strategist_fees
FROM {{ source('dlt', 'beefy_clm_subgraph___clms') }} t FINAL
