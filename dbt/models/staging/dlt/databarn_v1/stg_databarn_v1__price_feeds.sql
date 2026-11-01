{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.price_feed_id as Int64) AS price_feed_id,
  cast(t.feed_key as String) AS price_feed_key,
  cast(t.from_asset_key as String) AS from_asset_key,
  cast(t.to_asset_key as String) AS to_asset_key,
  cast(t.price_feed_data as String) AS price_feed_data
FROM {{ source('dlt', 'databarn_v1___price_feeds') }} t FINAL
