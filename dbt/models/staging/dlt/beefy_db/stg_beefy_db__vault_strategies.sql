{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.id as String) as id,
  cast(t.chain_id as String) as chain_id,
  cast(t.block_number as Int64) as block_number,
  {{ evm_transaction_hash('t.txn_hash') }} as txn_hash,
  cast(t.vault_id as String) as vault_id,
  {{ evm_address('t.vault_address') }} as vault_address,
  {{ evm_address('t.strategy_address') }} as strategy_address
FROM {{ source('dlt', 'beefy_db___vault_strategies') }} t FINAL

