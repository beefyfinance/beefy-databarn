{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  cast(t.block_number as Int64) as block_number,
  cast(lower(trim(t.txn_hash)) as String) as txn_hash,
  cast(lower(trim(t.from_address)) as String) as from_address,
  cast(t.input as String) as input,
  cast(t.created_at as DateTime('UTC')) as created_at
FROM {{ source('dlt', 'beefy_db___zap_failures') }} t
