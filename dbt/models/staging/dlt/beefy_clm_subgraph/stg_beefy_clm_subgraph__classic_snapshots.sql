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
  cast(t.period as Int64) AS period_seconds,
  cast(t.rounded_timestamp as DateTime('UTC')) AS period_start_at,
  cast(t.timestamp as DateTime('UTC')) AS last_updated_at,
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
FROM {{ source('dlt', 'beefy_clm_subgraph___classic_snapshots') }} t FINAL
