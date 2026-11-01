{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.investor_id as Int64) AS investor_id,
  cast({{ evm_address('t.address') }} as String) AS investor_address
FROM {{ source('dlt', 'databarn_v1___investors') }} t FINAL
