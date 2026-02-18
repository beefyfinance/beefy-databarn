{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t._dlt_id as String) as _dlt_id,
  cast(t.chain_id as String) as chain_id,
  cast(t.block_number as Int64) as block_number,
  cast(t.txn_timestamp as DateTime('UTC')) as txn_timestamp,
  {{ evm_transaction_hash('t.txn_hash') }} as txn_hash,
  {{ to_decimal('t.harvest_usd') }} as harvest_usd,
  {{ to_decimal('t.treasury_amt') }} as treasury_amt,
  {{ to_decimal('t.rewardpool_amt') }} as rewardpool_amt,
  {{ to_decimal('t.native_price') }} as native_price,
  {{ to_decimal('t.stable_price') }} as stable_price
FROM {{ source('dlt', 'beefy_db___feebatch_harvests') }} t
WHERE
  -- Filter out invalid records (ensure revenue data quality)
  t.treasury_amt IS NOT NULL
  AND t.txn_timestamp IS NOT NULL
  AND t.treasury_amt > 0
  -- Filter out invalid timestamps that would convert to 1970-01-01
  AND toDate(t.txn_timestamp) > '1970-01-01'

