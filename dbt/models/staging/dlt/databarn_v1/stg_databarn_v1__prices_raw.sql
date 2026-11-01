{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.datetime as DateTime('UTC')) AS datetime,
  cast(t.price_feed_id as Int64) AS price_feed_id,
  cast(t.block_number as Int64) AS block_number,
  {{ to_decimal('t.price') }} AS price
FROM {{ source('dlt', 'databarn_v1___prices_raw') }} t FINAL
