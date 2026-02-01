{{
  config(
    materialized='table',
    tags=['marts', 'tvl', 'stats'],
    order_by=['chain_id', 'product_address'],
  )
}}

-- 4 LEFT JOINs with only technical columns (chain_id, product_address, period, tvl_ago)
WITH technical_1d AS (
  SELECT c.chain_id, c.product_address, '1d' AS period, ps.tvl_usd AS tvl_ago
  FROM {{ ref('product_stats_latest') }} c
  LEFT JOIN {{ ref('product_stats') }} ps
    ON c.chain_id = ps.chain_id AND c.product_address = ps.product_address
   AND ps.date_hour = date_trunc('hour', now()) - INTERVAL 1 DAY
),
technical_1w AS (
  SELECT c.chain_id, c.product_address, '1w' AS period, ps.tvl_usd AS tvl_ago
  FROM {{ ref('product_stats_latest') }} c
  LEFT JOIN {{ ref('product_stats') }} ps
    ON c.chain_id = ps.chain_id AND c.product_address = ps.product_address
   AND ps.date_hour = date_trunc('hour', now()) - INTERVAL 7 DAY
),
technical_1m AS (
  SELECT c.chain_id, c.product_address, '1m' AS period, ps.tvl_usd AS tvl_ago
  FROM {{ ref('product_stats_latest') }} c
  LEFT JOIN {{ ref('product_stats') }} ps
    ON c.chain_id = ps.chain_id AND c.product_address = ps.product_address
   AND ps.date_hour = date_trunc('hour', now()) - INTERVAL 30 DAY
),
technical_1y AS (
  SELECT c.chain_id, c.product_address, '1y' AS period, ps.tvl_usd AS tvl_ago
  FROM {{ ref('product_stats_latest') }} c
  LEFT JOIN {{ ref('product_stats') }} ps
    ON c.chain_id = ps.chain_id AND c.product_address = ps.product_address
   AND ps.date_hour = date_trunc('hour', now()) - INTERVAL 365 DAY
),

unioned AS (
  SELECT * FROM technical_1d
  UNION ALL
  SELECT * FROM technical_1w
  UNION ALL
  SELECT * FROM technical_1m
  UNION ALL
  SELECT * FROM technical_1y
),

pivoted AS (
  SELECT
    chain_id,
    product_address,
    maxIf(tvl_ago, period = '1d') AS tvl_usd_1d_ago,
    maxIf(tvl_ago, period = '1w') AS tvl_usd_1w_ago,
    maxIf(tvl_ago, period = '1m') AS tvl_usd_1m_ago,
    maxIf(tvl_ago, period = '1y') AS tvl_usd_1y_ago
  FROM unioned
  GROUP BY chain_id, product_address
)

-- Enhance with display columns from product_stats_latest, then diffs and ranks
SELECT
  c.chain_id AS chain_id,
  c.product_address AS product_address,
  c.product_type AS product_type,
  c.beefy_key AS beefy_key,
  c.display_name AS display_name,
  c.is_active AS is_active,
  c.platform_id AS platform_id,
  c.date_hour AS ref_hour,
  c.tvl_usd AS tvl_usd_current,
  p.tvl_usd_1d_ago,
  p.tvl_usd_1w_ago,
  p.tvl_usd_1m_ago,
  p.tvl_usd_1y_ago,
  c.tvl_usd - p.tvl_usd_1d_ago AS tvl_usd_diff_1d,
  c.tvl_usd - p.tvl_usd_1w_ago AS tvl_usd_diff_1w,
  c.tvl_usd - p.tvl_usd_1m_ago AS tvl_usd_diff_1m,
  c.tvl_usd - p.tvl_usd_1y_ago AS tvl_usd_diff_1y,
  row_number() OVER w_1d_gainer AS rank_1d_gainer,
  row_number() OVER w_1d_loser AS rank_1d_loser,
  row_number() OVER w_1w_gainer AS rank_1w_gainer,
  row_number() OVER w_1w_loser AS rank_1w_loser,
  row_number() OVER w_1m_gainer AS rank_1m_gainer,
  row_number() OVER w_1m_loser AS rank_1m_loser,
  row_number() OVER w_1y_gainer AS rank_1y_gainer,
  row_number() OVER w_1y_loser AS rank_1y_loser
FROM {{ ref('product_stats_latest') }} c
INNER JOIN pivoted p ON c.chain_id = p.chain_id AND c.product_address = p.product_address
WINDOW
  w_1d_gainer AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1d_ago, -1e308) DESC),
  w_1d_loser AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1d_ago, 1e308) ASC),
  w_1w_gainer AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1w_ago, -1e308) DESC),
  w_1w_loser AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1w_ago, 1e308) ASC),
  w_1m_gainer AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1m_ago, -1e308) DESC),
  w_1m_loser AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1m_ago, 1e308) ASC),
  w_1y_gainer AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1y_ago, -1e308) DESC),
  w_1y_loser AS (ORDER BY coalesce(c.tvl_usd - p.tvl_usd_1y_ago, 1e308) ASC)
