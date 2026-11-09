{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['date_day'],
    tags=['revenue'],
  )
}}

-- Daily totals for the revenue dashboard and api_revenue_summary, over the whole history:
-- - revenue_usd: treasury share of fee batch harvests (fact_fee_batch_obs)
-- - yield_usd: want compounded by harvests, in USD (fact_harvest_obs)
-- - bifi_buyback_usd: BIFI bought back, in USD (fact_buyback_obs)

WITH all_rows AS (
  SELECT toDate(txn_timestamp) AS date_day, toDecimal256(harvest_amount_usd, 20) AS yield_usd,
    toDecimal256(0, 20) AS revenue_usd, toDecimal256(0, 20) AS bifi_buyback_usd
  FROM {{ ref('fact_harvest_obs') }}
  UNION ALL
  SELECT toDate(txn_timestamp), toDecimal256(0, 20), toDecimal256(ifNull(treasury_usd, 0), 20), toDecimal256(0, 20)
  FROM {{ ref('fact_fee_batch_obs') }}
  UNION ALL
  SELECT toDate(txn_timestamp), toDecimal256(0, 20), toDecimal256(0, 20), toDecimal256(buyback_usd, 20)
  FROM {{ ref('fact_buyback_obs') }}
)

SELECT
  date_day,
  sum(revenue_usd) AS revenue_usd,
  sum(yield_usd) AS yield_usd,
  sum(bifi_buyback_usd) AS bifi_buyback_usd
FROM all_rows
GROUP BY date_day
