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
  {{ evm_address('t.investor') }} AS investor_address,
  t.investor_position AS investor_position_key,
  t.type AS interaction_type,
  toInt256(t.manager_balance) AS manager_balance,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_pool_balances') }}) AS reward_pool_balances,
  toInt256(t.total_balance) AS total_balance,
  toInt256(t.underlying_balance0) AS underlying_balance0,
  toInt256(t.underlying_balance1) AS underlying_balance1,
  toInt256(t.manager_balance_delta) AS manager_balance_delta,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_pool_balances_delta') }}) AS reward_pool_balances_delta,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_balances_delta') }}) AS reward_balances_delta,
  {{ evm_address('t.claimed_reward_pool') }} AS claimed_reward_pool_address,
  toInt256(t.underlying_balance0_delta) AS underlying_balance0_delta,
  toInt256(t.underlying_balance1_delta) AS underlying_balance1_delta,
  toInt256(t.token0_to_native_price) AS token0_to_native_price,
  toInt256(t.token1_to_native_price) AS token1_to_native_price,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.output_to_native_prices') }}) AS output_to_native_prices,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_to_native_prices') }}) AS reward_to_native_prices,
  toInt256(t.native_to_usd_price) AS native_to_usd_price
FROM {{ source('dlt', 'beefy_clm_subgraph___clm_position_interactions') }} t FINAL
