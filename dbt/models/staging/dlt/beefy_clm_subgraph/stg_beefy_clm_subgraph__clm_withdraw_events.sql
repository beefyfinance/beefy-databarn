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
  toInt256(t.shares) AS shares,
  toInt256(t.amount0) AS amount0,
  toInt256(t.amount1) AS amount1
FROM {{ source('dlt', 'beefy_clm_subgraph___clm_withdraw_events') }} t FINAL
