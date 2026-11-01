{{
  config(
    materialized='view',
  )
}}

-- Amounts and prices are raw on-chain integers: apply token decimals downstream.
SELECT
  t.chain_key AS chain_key,
  {{ evm_transaction_hash('t.id') }} AS trx_hash,
  cast(t.block_number as Int64) AS block_number,
  cast(t.block_timestamp as DateTime('UTC')) AS block_timestamp,
  {{ evm_address('t.sender') }} AS sender_address
FROM {{ source('dlt', 'beefy_clm_subgraph___transactions') }} t FINAL
