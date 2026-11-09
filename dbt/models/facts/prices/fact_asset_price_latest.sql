{{
  config(
    materialized='view',
    tags=['prices'],
  )
}}

-- Latest price of every asset priced in the last day (the dense table has a row every 5 minutes while a source is
-- fresh, so one day of it holds every live asset). as_of is the slot of the row; observed_at the slot of the value it
-- holds or carries, so a carried price shows its real age (now() - observed_at).
SELECT
  p.asset_id AS asset_id,
  max(p.date_time) AS as_of,
  argMax(p.price, p.date_time) AS price,
  argMax(p.price_source, p.date_time) AS price_source,
  argMax(p.observed_at, p.date_time) AS observed_at
FROM {{ ref('fact_asset_price_5m') }} p
WHERE p.date_time >= now() - INTERVAL 1 DAY
GROUP BY p.asset_id
