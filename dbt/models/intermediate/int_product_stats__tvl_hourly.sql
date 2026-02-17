{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}


-- Materialized intermediate: Hourly tvl aggregations
-- This reduces memory usage by materializing tvl aggregations separately
-- Combines tvl staging data with product information and aggregates to hourly

WITH tvl_with_product AS (
  SELECT
    p.chain_id,
    p.product_address,
    a.date_time,
    a.tvl_usd
  FROM {{ ref('stg_beefy_db__tvls') }} a
  INNER JOIN {{ ref('stg_beefy_db__vault_ids') }} vi
    ON a.vault_id = vi.vault_id
  INNER JOIN {{ ref('product') }} p
    ON vi.beefy_key = p.beefy_key
  WHERE 
    a.tvl_usd between 0 and 1000000000
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(tvl_usd, date_time) as tvl_usd
FROM tvl_with_product
GROUP BY chain_id, product_address, toStartOfHour(date_time)