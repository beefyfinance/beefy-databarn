{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.etag as String) as etag,
  cast(t.network_id as Int64) as network_id,
  cast(t.vault_id as String) as vault_id,
  toFloat64(t.tvl) as tvl,
  cast(t.date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api___tvl') }} t FINAL

