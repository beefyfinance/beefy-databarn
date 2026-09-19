{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.etag as String) as etag,
  cast(t.chain_id as String) as chain_id,
  ifNull(t.moo_token_symbol, 'Unknown') as moo_token_symbol,
  toFloat64(t.price) as price,
  cast(t.date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api___mootokenprices') }} t FINAL
where t.price is not null
