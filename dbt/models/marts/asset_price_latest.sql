{{
  config(
    materialized='view',
    tags=['marts', 'prices'],
  )
}}

-- Latest price of every asset priced in the last day (the dense table has a row every 5 minutes while a source is
-- fresh, so one day of it holds every live asset).
SELECT
  p.asset_id AS asset_id,
  max(p.date_time) AS date_time,
  argMax(p.price, p.date_time) AS price,
  argMax(p.price_source, p.date_time) AS price_source
FROM {{ ref('asset_price_5m') }} p
WHERE p.date_time >= now() - INTERVAL 1 DAY
GROUP BY p.asset_id
