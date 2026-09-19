{{
  config(
    materialized='view',
  )
}}

SELECT
  t.chain_id as network_id,
  {{ evm_address('t.address') }} as address,
  toBool(t.is_contract) as is_contract,
  nullif(t.label, '') as label
FROM {{ source('dlt', 'beefy_db___address_metadata') }} t FINAL

