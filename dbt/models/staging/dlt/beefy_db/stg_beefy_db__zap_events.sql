{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  cast(t.block_number as Int64) as block_number,
  cast(t.txn_idx as Int32) as txn_idx,
  cast(t.event_idx as Int32) as event_idx,
  cast(t.txn_timestamp as DateTime('UTC')) as txn_timestamp,
  cast(lower({{ evm_transaction_hash('t.txn_hash') }}) as String) as txn_hash,
  cast({{ evm_address('t.caller_address') }} as String) as caller_address,
  cast({{ evm_address('t.recipient_address') }} as String) as recipient_address,
  cast(t.target_chain_id as Int64) as target_chain_id,
  cast(t.vault_id as Nullable(String)) as vault_id,
  cast(t.action as Nullable(String)) as action,
  cast(t.swap_source as Array(Nullable(String))) as swap_source,
  toDecimal256(ifNull({{ to_decimal('t.total_usd') }}, 0), 20) as total_usd,
  cast(t.updated_at as DateTime('UTC')) as updated_at
FROM {{ source('dlt', 'beefy_db___zap_events') }} t FINAL
