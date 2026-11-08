{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'harvest'],
    order_by=['txn_timestamp', 'chain_id', 'product_address', 'block_number', 'txn_idx', 'event_idx'],
  )
}}

-- Mart: one row per harvest, joined to product dimension only. No grouping.
-- Computed USD columns: fee * native_price, harvest_amount * want_price, platform fee * platform_fee_price
-- (frxETH on Fraxtal). Prices come from int_harvests (asset_price_5m on the harvest's slot).

SELECT
  h.txn_timestamp AS txn_timestamp,
  p.chain_id AS chain_id,
  p.product_address AS product_address,
  p.beefy_key AS beefy_key,
  p.product_type AS product_type,
  p.display_name AS display_name,
  h.block_number AS block_number,
  h.txn_idx AS txn_idx,
  h.event_idx AS event_idx,
  h.txn_hash AS txn_hash,
  h.call_fee AS harvest_call_fee,
  toDecimal256(h.call_fee * h.native_price, 20) AS harvest_call_fee_usd,
  h.gas_fee AS gas_fee,
  toDecimal256(h.gas_fee * h.native_price, 20) AS gas_fee_usd,
  h.platform_fee AS platform_fee,
  toDecimal256(h.platform_fee * h.platform_fee_price, 20) AS platform_fee_usd,
  h.strategist_fee AS strategist_fee,
  toDecimal256(h.strategist_fee * h.native_price, 20) AS strategist_fee_usd,
  h.harvest_amount AS harvest_amount,
  toDecimal256(h.harvest_amount * h.want_price, 20) AS harvest_amount_usd,
  h.native_price AS avg_native_price
FROM {{ ref('int_harvests') }} h
INNER JOIN {{ ref('product') }} p
  ON h.network_id = p.chain_id
  AND h.vault_beefy_key = p.beefy_key
