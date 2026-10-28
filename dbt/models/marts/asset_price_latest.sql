{{
  config(
    materialized='view',
    tags=['marts', 'prices'],
  )
}}

-- Latest known price per asset (last 15 days).
SELECT
  asset_id,
  max(date_time) as date_time,
  argMax(price, date_time) as price
FROM {{ ref('asset_price_5m') }}
WHERE date_time >= now() - INTERVAL 15 DAY
GROUP BY asset_id
