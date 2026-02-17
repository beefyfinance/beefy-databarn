{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['txn_timestamp', 'network_id', 'block_number', 'txn_idx', 'event_idx'],
  )
}}

-- Staging as table + MV so int_yield_mv can fire on insert (trigger: dlt.beefy_db___harvests).
SELECT
  cast(t.chain_id as Int64) as network_id,
  cast(t.block_number as Int64) as block_number,
  cast(t.txn_idx as Int32) as txn_idx,
  cast(t.event_idx as Int32) as event_idx,
  cast(t.txn_timestamp as DateTime('UTC')) as txn_timestamp,
  cast(lower({{ evm_transaction_hash('t.txn_hash') }}) as String) as txn_hash,
  cast(t.vault_id as String) as vault_beefy_key,
  toDecimal256(ifNull({{ to_decimal('t.call_fee') }}, 0), 20) as call_fee,
  toDecimal256(ifNull({{ to_decimal('t.gas_fee') }}, 0), 20) as gas_fee,
  toDecimal256(ifNull({{ to_decimal('t.platform_fee') }}, 0), 20) as platform_fee,
  toDecimal256(ifNull({{ to_decimal('t.strategist_fee') }}, 0), 20) as strategist_fee,
  toDecimal256(ifNull({{ to_decimal('t.harvest_amount') }}, 0), 20) as harvest_amount,
  toDecimal256(ifNull({{ to_decimal('t.native_price') }}, 0), 20) as native_price,
  toDecimal256(ifNull({{ to_decimal('t.want_price') }}, 0), 20) as want_price,
  toBool(ifNull(t.is_cowllector, false)) as is_cowllector,
  cast({{ evm_address('t.strategist_address') }} as Nullable(String)) as strategist_address
FROM {{ source('dlt', 'beefy_db___harvests') }} t
