{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['txn_timestamp', 'block_number', 'event_idx'],
    tags=['revenue']
  )
}}

-- Intermediate model: Transform BIFI buyback events into revenue structure
-- This model maps buyback events to a revenue model, where buyback_total represents revenue
-- Handles data quality issues, standardizes formats, and prepares for revenue aggregation
-- Joins with token dimension to provide token metadata
-- token_price_usd is the BIFI price of fact_asset_price_5m on the buyback's 5-minute slot (the price beefy-db stored with
-- the buyback is the beefy_db BIFI price, so the two agree), else the stored price. A table priced in full each run.

WITH cleaned_revenue AS (
  SELECT
    bb.id,
    bb.block_number,
    -- Standardize timestamp (handle timezone issues if any)
    toDateTime(bb.txn_timestamp) as txn_timestamp,
    bb.event_idx,
    lower(bb.txn_hash) as txn_hash,
    bb.bifi_price as stored_price_usd,
    bb.bifi_amount as buyback_amount
  FROM {{ ref('stg_beefy_db__bifi_buyback') }} bb
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    buyback_total IS NOT NULL
    AND txn_timestamp IS NOT NULL
    AND bifi_amount > 0
    AND bifi_price > 0
)

SELECT
  cr.id as id,
  cr.txn_timestamp,
  cr.block_number,
  cr.event_idx,
  cr.txn_hash,
  t.chain_id,
  t.representation_address as token_representation_address,
  t.erc20_address as token_erc20_address,
  t.is_native as token_is_native,
  t.symbol as token_symbol,
  t.name as token_name,
  t.decimals as token_decimals,
  toDecimal256(if(p.price > 0, p.price, toFloat64(cr.stored_price_usd)), 20) as token_price_usd,
  cr.buyback_amount as buyback_amount,
  cr.buyback_amount * toDecimal256(if(p.price > 0, p.price, toFloat64(cr.stored_price_usd)), 20) as buyback_usd
FROM cleaned_revenue cr
INNER JOIN {{ ref('dim_token') }} t
  -- BIFI exists on several chains, buybacks only happen on Ethereum: block numbers and spacing match it since 2023.
  ON t.symbol = 'BIFI'
  AND t.chain_id = 1
-- join_use_nulls = 0: a miss has price 0.
LEFT JOIN (
  SELECT asset_id, date_time, price FROM {{ ref('fact_asset_price_5m') }}
  WHERE asset_id IN (SELECT asset_id FROM {{ ref('dim_token') }} WHERE symbol = 'BIFI' AND chain_id = 1)
) p
  ON p.asset_id = t.asset_id
  AND p.date_time = toStartOfFiveMinutes(cr.txn_timestamp)
