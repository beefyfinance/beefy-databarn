{{
  config(
    materialized='view',
  )
}}

-- Amounts and prices are raw on-chain integers: apply token decimals downstream.
SELECT
  t.chain_key AS chain_key,
  t.id AS subgraph_key,
  {{ evm_address('t.classic') }} AS classic_address,
  {{ evm_transaction_hash('t.created_with') }} AS trx_hash,
  cast(t.log_index as Int64) AS log_index,
  cast(t.timestamp as DateTime('UTC')) AS block_timestamp,
  cast(t.block_number as Int64) AS block_number,
  {{ evm_address('t.investor') }} AS investor_address,
  t.investor_position AS investor_position_key,
  t.type AS interaction_type,
  toInt256(t.vault_balance) AS vault_balance,
  toInt256(t.boost_balance) AS boost_balance,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_pool_balances') }}) AS reward_pool_balances,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.erc4626_adapter_balances') }}) AS erc4626_adapter_balances,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.erc4626_adapter_vault_shares_balances') }}) AS erc4626_adapter_vault_shares_balances,
  toInt256(t.total_balance) AS total_balance,
  toInt256(t.vault_shares_total_supply) AS vault_shares_total_supply,
  toInt256(t.vault_underlying_total_supply) AS vault_underlying_total_supply,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.vault_underlying_breakdown_balances') }}) AS vault_underlying_breakdown_balances,
  toInt256(t.vault_underlying_amount) AS vault_underlying_amount,
  toInt256(t.vault_underlying_balance) AS vault_underlying_balance,
  toInt256(t.vault_balance_delta) AS vault_balance_delta,
  toInt256(t.boost_balance_delta) AS boost_balance_delta,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.boost_reward_balances_delta') }}) AS boost_reward_balances_delta,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_pool_balances_delta') }}) AS reward_pool_balances_delta,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_balances_delta') }}) AS reward_balances_delta,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.erc4626_adapter_balances_delta') }}) AS erc4626_adapter_balances_delta,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.erc4626_adapter_vault_shares_balances_delta') }}) AS erc4626_adapter_vault_shares_balances_delta,
  toInt256(t.underlying_to_native_price) AS underlying_to_native_price,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.underlying_breakdown_to_native_prices') }}) AS underlying_breakdown_to_native_prices,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.boost_reward_to_native_prices') }}) AS boost_reward_to_native_prices,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_to_native_prices') }}) AS reward_to_native_prices,
  toInt256(t.native_to_usd_price) AS native_to_usd_price
FROM {{ source('dlt', 'beefy_clm_subgraph___classic_position_interactions') }} t FINAL
