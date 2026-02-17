{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}


-- Materialized intermediate: Hourly APY aggregations
-- This reduces memory usage by materializing APY aggregations separately
-- Combines APY staging data with product information and aggregates to hourly

WITH apy_with_product AS (
  SELECT
    p.chain_id,
    p.product_address,
    a.date_time,
    a.apy
  FROM {{ ref('stg_beefy_db__apys') }} a
  INNER JOIN {{ ref('stg_beefy_db__vault_ids') }} vi
    ON a.vault_id = vi.vault_id
  INNER JOIN {{ ref('product') }} p
    ON vi.beefy_key = p.beefy_key
  WHERE 
    a.apy between 0 and 1000000 -- no one product has an apy over 1M %
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(apy, date_time) as apy
FROM apy_with_product
GROUP BY chain_id, product_address, toStartOfHour(date_time)
