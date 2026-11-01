{{
  config(
    materialized='view',
  )
}}

-- Amounts and prices are raw on-chain integers: apply token decimals downstream.
SELECT
  t.chain_key AS chain_key,
  t.id AS subgraph_key,
  {{ evm_address('t.clm') }} AS clm_address,
  {{ evm_transaction_hash('t.created_with') }} AS trx_hash,
  cast(t.log_index as Int64) AS log_index,
  cast(t.timestamp as DateTime('UTC')) AS block_timestamp,
  cast(t.block_number as Int64) AS block_number,
  {{ evm_address('t.strategy') }} AS strategy_address,
  toInt256(t.underlying_amount0) AS underlying_amount0,
  toInt256(t.underlying_amount1) AS underlying_amount1,
  toInt256(t.compounded_amount0) AS compounded_amount0,
  toInt256(t.compounded_amount1) AS compounded_amount1,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.collected_output_amounts') }}) AS collected_output_amounts,
  toInt256(t.manager_total_supply) AS manager_total_supply,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_pools_total_supply') }}) AS reward_pools_total_supply,
  toInt256(t.token0_to_native_price) AS token0_to_native_price,
  toInt256(t.token1_to_native_price) AS token1_to_native_price,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.output_to_native_prices') }}) AS output_to_native_prices,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_to_native_prices') }}) AS reward_to_native_prices,
  toInt256(t.native_to_usd_price) AS native_to_usd_price
FROM {{ source('dlt', 'beefy_clm_subgraph___clm_harvest_events') }} t FINAL
