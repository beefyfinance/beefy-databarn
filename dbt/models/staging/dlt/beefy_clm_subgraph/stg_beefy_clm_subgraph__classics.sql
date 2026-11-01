{{
  config(
    materialized='view',
  )
}}

-- Amounts and prices are raw on-chain integers: apply token decimals downstream.
SELECT
  t.chain_key AS chain_key,
  {{ evm_address('t.id') }} AS classic_address,
  t.lifecycle AS lifecycle,
  {{ evm_address('t.vault') }} AS vault_address,
  t.underlying_platform AS underlying_platform,
  {{ evm_address('t.vault_shares_token') }} AS vault_shares_token_address,
  {{ evm_address('t.underlying_token') }} AS underlying_token_address,
  {{ to_str_list('t.underlying_breakdown_tokens_order') }} AS underlying_breakdown_tokens_order,
  {{ to_str_list('t.boost_reward_tokens_order') }} AS boost_reward_tokens_order,
  {{ to_str_list('t.reward_tokens_order') }} AS reward_tokens_order,
  {{ to_str_list('t.reward_pool_tokens_order') }} AS reward_pool_tokens_order,
  {{ to_str_list('t.erc4626_adapter_tokens_order') }} AS erc4626_adapter_tokens_order,
  toInt256(t.vault_shares_total_supply) AS vault_shares_total_supply,
  toInt256(t.vault_underlying_total_supply) AS vault_underlying_total_supply,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.vault_underlying_breakdown_balances') }}) AS vault_underlying_breakdown_balances,
  toInt256(t.vault_underlying_balance) AS vault_underlying_balance,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_pools_total_supply') }}) AS reward_pools_total_supply,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.erc4626_adapters_total_supply') }}) AS erc4626_adapters_total_supply,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.erc4626_adapter_vault_shares_balances') }}) AS erc4626_adapter_vault_shares_balances,
  toInt256(t.underlying_to_native_price) AS underlying_to_native_price,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.underlying_breakdown_to_native_prices') }}) AS underlying_breakdown_to_native_prices,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.boost_reward_to_native_prices') }}) AS boost_reward_to_native_prices,
  arrayMap(x -> toInt256(x), {{ to_str_list('t.reward_to_native_prices') }}) AS reward_to_native_prices,
  toInt256(t.native_to_usd_price) AS native_to_usd_price,
  toInt256(t.underlying_amount) AS underlying_amount,
  toInt256(t.total_call_fees) AS total_call_fees,
  toInt256(t.total_beefy_fees) AS total_beefy_fees,
  toInt256(t.total_strategist_fees) AS total_strategist_fees
FROM {{ source('dlt', 'beefy_clm_subgraph___classics') }} t FINAL
