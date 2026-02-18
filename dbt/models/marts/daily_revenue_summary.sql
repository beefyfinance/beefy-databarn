{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'revenue', 'daily']
  )
}}

-- Mart model: Daily aggregation of revenue, yield, and BIFI buyback metrics (last 30 days)
-- This model provides daily totals for:
-- - revenue_usd: Total revenue from BIFI buybacks and feebatch harvests (treasury amounts)
-- - yield_usd: Total yield from harvest events
-- - bifi_buyback_usd: Total BIFI buyback amount in USD
-- Limited to the last 30 days for performance
-- Built directly from staging models without relying on other marts

WITH bifi_buyback_daily AS (
  SELECT
    toDate(bb.txn_timestamp) as date_day,
    0 as yield_usd,
    0 as revenue_usd,
    sum(toDecimal256(bb.bifi_amount * bb.bifi_price, 20)) as bifi_buyback_usd
  FROM {{ ref('stg_beefy_db__bifi_buyback') }} bb
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    bb.buyback_total IS NOT NULL
    AND bb.txn_timestamp IS NOT NULL
    AND bb.bifi_amount > 0
    AND bb.bifi_price > 0
    -- Filter out invalid timestamps that would convert to 1970-01-01
    AND toDate(bb.txn_timestamp) > '1970-01-01'
  GROUP BY toDate(bb.txn_timestamp)
),

feebatch_revenue_daily AS (
  SELECT
    toDate(fh.txn_timestamp) as date_day,
    0 as yield_usd,
    sum(toDecimal256(fh.treasury_amt, 20)) as revenue_usd,
    0 as bifi_buyback_usd
  FROM {{ ref('stg_beefy_db__feebatch_harvests') }} fh
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    fh.treasury_amt IS NOT NULL
    AND fh.txn_timestamp IS NOT NULL
    AND fh.treasury_amt > 0
    -- Filter out invalid timestamps that would convert to 1970-01-01
    AND toDate(fh.txn_timestamp) > '1970-01-01'
  GROUP BY toDate(fh.txn_timestamp)
),

yield_daily AS (
  SELECT
    toDate(ye.date_time) as date_day,
    sum(ye.underlying_amount_compounded_usd) as yield_usd,
    0 as revenue_usd,
    0 as bifi_buyback_usd
  FROM {{ ref('int_yield') }} ye
  WHERE
    -- Filter out invalid timestamps that would convert to 1970-01-01
    ye.date_time IS NOT NULL
    AND toDate(ye.date_time) > '1970-01-01'
  GROUP BY toDate(ye.date_time)
),

all_rows as (
  select * from yield_daily
  union all
  select * from feebatch_revenue_daily
  union all
  select * from bifi_buyback_daily
)

SELECT
  date_day as date_day,
  coalesce(sum(yield_usd), 0) as yield_usd,
  coalesce(sum(revenue_usd), 0) as revenue_usd,
  coalesce(sum(bifi_buyback_usd), 0) as bifi_buyback_usd
FROM all_rows
GROUP BY date_day
ORDER BY date_day DESC

