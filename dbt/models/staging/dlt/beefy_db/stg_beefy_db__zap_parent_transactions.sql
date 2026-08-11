{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  cast(t.block_number as Int64) as block_number,
  cast(t.txn_idx as Int32) as txn_idx,
  cast(t.txn_timestamp as DateTime('UTC')) as txn_timestamp,
  cast(lower({{ evm_transaction_hash('t.txn_hash') }}) as String) as txn_hash,
  toDecimal256(ifNull({{ to_decimal('t.value') }}, 0), 20) as value,
  cast(t.effective_gas_price as Int64) as effective_gas_price,
  cast(t.gas_limit as Int64) as gas_limit,
  cast(t.gas_used as Int64) as gas_used,
  toDecimal256(ifNull({{ to_decimal('t.native_price') }}, 0), 20) as native_price,
  cast({{ evm_address('t.from_address') }} as String) as from_address,
  cast({{ evm_address('t.to_address') }} as String) as to_address,
  cast(t.calldata as String) as calldata,
  toBool(t.success) as success,
  cast(t.created_at as DateTime('UTC')) as created_at
FROM {{ source('dlt', 'beefy_db___zap_parent_transactions') }} t
