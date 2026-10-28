{{
  config(
    materialized='view',
    tags=['marts', 'prices'],
  )
}}

-- Latest known price per asset (last 15 days).
-- Qualified p.date_time: the output alias date_time would otherwise shadow the column in WHERE.
SELECT
  p.asset_id as asset_id,
  max(p.date_time) as date_time,
  argMax(p.price, p.date_time) as price
FROM {{ ref('asset_price_5m') }} p
WHERE p.date_time >= now() - INTERVAL 15 DAY
GROUP BY p.asset_id
