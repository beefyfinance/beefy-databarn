{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.product_id as Int64) AS product_id,
  cast(t.product_key as String) AS product_key,
  cast(t.chain as String) AS chain_key,
  cast(t.price_feed_1_id as Nullable(Int64)) AS price_feed_1_id,
  cast(t.price_feed_2_id as Nullable(Int64)) AS price_feed_2_id,
  cast(t.pending_rewards_price_feed_id as Nullable(Int64)) AS pending_rewards_price_feed_id,
  cast(t.product_data as String) AS product_data
FROM {{ source('dlt', 'databarn_v1___products') }} t FINAL
