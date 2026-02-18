{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.id as String) as id,
  cast(t.block_number as Int64) as block_number,
  cast(t.txn_timestamp as DateTime('UTC')) as txn_timestamp,
  cast(t.event_idx as Int64) as event_idx,
  {{ evm_transaction_hash('t.txn_hash') }} as txn_hash,
  {{ to_decimal('t.bifi_amount') }} as bifi_amount,
  {{ to_decimal('t.bifi_price') }} as bifi_price,
  {{ to_decimal('t.buyback_total') }} as buyback_total
FROM {{ source('dlt', 'beefy_db___bifi_buyback') }} t
WHERE
  -- Filter out invalid records (ensure revenue data quality)
  t.buyback_total IS NOT NULL
  AND t.txn_timestamp IS NOT NULL
  AND t.bifi_amount > 0
  AND t.bifi_price > 0
  -- Filter out invalid timestamps that would convert to 1970-01-01
  AND toDate(t.txn_timestamp) > '1970-01-01'
