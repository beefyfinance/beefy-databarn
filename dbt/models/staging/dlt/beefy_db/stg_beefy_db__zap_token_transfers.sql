{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  cast(t.block_number as Int64) as block_number,
  cast(t.txn_idx as Int32) as txn_idx,
  cast(t.parent_event_idx as Int32) as parent_event_idx,
  cast(t.event_idx as Int32) as event_idx,
  cast(t.zap_type as String) as zap_type,
  cast(t.token_id as Nullable(Int64)) as token_id,
  toDecimal256(ifNull({{ to_decimal('t.token_amount') }}, 0), 20) as token_amount,
  toDecimal256(ifNull({{ to_decimal('t.usd_value') }}, 0), 20) as usd_value,
  cast({{ evm_address('t.token_address') }} as String) as token_address,
  cast(t.updated_at as DateTime('UTC')) as updated_at
FROM {{ source('dlt', 'beefy_db___zap_token_transfers') }} t FINAL
