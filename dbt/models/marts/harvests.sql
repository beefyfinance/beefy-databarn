{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'harvest'],
    order_by=['txn_timestamp', 'chain_id', 'product_address', 'block_number', 'txn_idx', 'event_idx'],
  )
}}

-- Mart: one row per harvest, joined to product dimension only. No grouping.
-- Computed USD columns: fee * native_price, harvest_amount * want_price.
-- Exception: Fraxtal (252) platform fees are paid in frxETH, not native FRAX.

WITH harvest_base AS (
  SELECT
    h.txn_timestamp,
    p.chain_id,
    p.product_address,
    p.beefy_key,
    p.product_type,
    p.display_name,
    h.block_number,
    h.txn_idx,
    h.event_idx,
    h.txn_hash,
    h.call_fee,
    h.gas_fee,
    h.platform_fee,
    h.strategist_fee,
    h.harvest_amount,
    h.native_price,
    h.want_price
  FROM {{ ref('stg_beefy_db__harvests') }} h
  INNER JOIN {{ ref('product') }} p
    ON h.network_id = p.chain_id
    AND h.vault_beefy_key = p.beefy_key
),
frxeth_price AS (
  SELECT
    toInt64(252) AS chain_id, -- fraxtal
    date_time,
    price
  FROM {{ ref('int_price') }}
  WHERE oracle_key = 'frxETH'
)

SELECT
  h.txn_timestamp,
  h.chain_id,
  h.product_address,
  h.beefy_key,
  h.product_type,
  h.display_name,
  h.block_number,
  h.txn_idx,
  h.event_idx,
  h.txn_hash,
  h.call_fee AS harvest_call_fee,
  toDecimal256(h.call_fee * h.native_price, 20) AS harvest_call_fee_usd,
  h.gas_fee,
  toDecimal256(h.gas_fee * h.native_price, 20) AS gas_fee_usd,
  h.platform_fee,
  toDecimal256(h.platform_fee * coalesce(frx.price, h.native_price), 20) AS platform_fee_usd,
  h.strategist_fee,
  toDecimal256(h.strategist_fee * h.native_price, 20) AS strategist_fee_usd,
  h.harvest_amount,
  toDecimal256(h.harvest_amount * h.want_price, 20) AS harvest_amount_usd,
  h.native_price AS avg_native_price
FROM harvest_base h
ASOF LEFT JOIN frxeth_price frx
  ON h.chain_id = frx.chain_id
  AND h.txn_timestamp >= frx.date_time
