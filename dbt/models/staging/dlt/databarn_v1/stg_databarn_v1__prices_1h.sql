{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.datetime as DateTime('UTC')) AS datetime,
  cast(t.price_feed_id as Int64) AS price_feed_id,
  {{ to_decimal('t.price_avg') }} AS price_avg,
  {{ to_decimal('t.price_high') }} AS price_high,
  {{ to_decimal('t.price_low') }} AS price_low,
  {{ to_decimal('t.price_open') }} AS price_open,
  {{ to_decimal('t.price_close') }} AS price_close
FROM {{ source('dlt', 'databarn_v1___prices_1h') }} t FINAL
