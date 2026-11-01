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
  {{ evm_address('t.strategy') }} AS strategy_address,
  toInt256(t.underlying_amount0) AS underlying_amount0,
  toInt256(t.underlying_amount1) AS underlying_amount1
FROM {{ source('dlt', 'beefy_clm_subgraph___clm_strategy_tvl_events') }} t FINAL
