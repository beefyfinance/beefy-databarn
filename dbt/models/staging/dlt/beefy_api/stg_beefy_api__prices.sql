{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.etag as String) as etag,
  t.token_symbol as token_symbol,
  toFloat64(t.price) as price,
  cast(t.date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api___prices') }} t FINAL

