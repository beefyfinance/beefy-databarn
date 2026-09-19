{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.etag as String) as etag,
  cast(t.vault_id as String) as vault_id,
  toFloat64(t.lps) as lps,
  cast(t.date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api___lps') }} t FINAL

