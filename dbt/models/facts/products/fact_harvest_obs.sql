{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['harvest'],
    order_by=['txn_timestamp', 'chain_id', 'product_address', 'block_number', 'txn_idx', 'event_idx'],
  )
}}

-- One row per harvest of a product, joined to the product dimension only. No grouping.
-- Computed USD columns: fee * native_price, harvest_amount * want_price, platform fee * platform_fee_price
-- (frxETH on Fraxtal). Prices come from int_harvests (fact_asset_price_5m on the harvest's slot).
-- Replaces yield_harvest_events, which held the same rows: its underlying_amount_compounded is harvest_amount, its
-- underlying_token_price_usd want_price and its underlying_amount_compounded_usd harvest_amount_usd.

SELECT
  h.txn_timestamp AS txn_timestamp,
  p.chain_id AS chain_id,
  toLowCardinality(p.product_address) AS product_address,
  toLowCardinality(p.beefy_key) AS beefy_key,
  toLowCardinality(p.product_type) AS product_type,
  toLowCardinality(p.display_name) AS display_name,
  h.block_number AS block_number,
  h.txn_idx AS txn_idx,
  h.event_idx AS event_idx,
  h.txn_hash AS txn_hash,
  h.call_fee AS harvest_call_fee,
  h.call_fee * h.native_price AS harvest_call_fee_usd,
  h.gas_fee AS gas_fee,
  h.gas_fee * h.native_price AS gas_fee_usd,
  h.platform_fee AS platform_fee,
  h.platform_fee * h.platform_fee_price AS platform_fee_usd,
  h.strategist_fee AS strategist_fee,
  h.strategist_fee * h.native_price AS strategist_fee_usd,
  h.harvest_amount AS harvest_amount,
  h.harvest_amount * h.want_price AS harvest_amount_usd,
  h.want_price AS want_price,
  h.native_price AS avg_native_price
FROM {{ ref('int_harvests') }} h
INNER JOIN {{ ref('dim_product') }} p
  ON h.network_id = p.chain_id
  AND h.vault_beefy_key = p.beefy_key
