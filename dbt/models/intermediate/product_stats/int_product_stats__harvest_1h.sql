{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'fact_product_stats_1h'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}

-- Prices come from int_harvests (fact_asset_price_5m on the harvest's slot; platform fees in frxETH on Fraxtal).
-- A table: rebuilt from int_harvests each run, so re-resolved prices reach every hour.
SELECT
  p.chain_id AS chain_id,
  toLowCardinality(p.product_address) AS product_address,
  toStartOfHour(h.txn_timestamp) AS date_hour,
  sum(h.call_fee) AS harvest_call_fee,
  sum(h.call_fee * h.native_price) AS harvest_call_fee_usd,
  sum(h.gas_fee) AS gas_fee,
  sum(h.gas_fee * h.native_price) AS gas_fee_usd,
  sum(h.platform_fee) AS platform_fee,
  sum(h.platform_fee * h.platform_fee_price) AS platform_fee_usd,
  sum(h.strategist_fee) AS strategist_fee,
  sum(h.strategist_fee * h.native_price) AS strategist_fee_usd,
  sum(h.harvest_amount) AS harvest_amount,
  sum(h.harvest_amount * h.want_price) AS harvest_amount_usd,
  avg(h.native_price) AS avg_native_price,
  count(DISTINCT h.txn_hash) AS harvest_txn_count,
  1 AS harvest_vault_count
FROM {{ ref('int_harvests') }} h
INNER JOIN {{ ref('int_product_keys') }} p
  ON h.network_id = p.chain_id
  AND h.vault_beefy_key = p.beefy_key
WHERE
  h.txn_timestamp IS NOT NULL
  AND toDate(h.txn_timestamp) > '1970-01-01'
GROUP BY p.chain_id, p.product_address, toStartOfHour(h.txn_timestamp)
