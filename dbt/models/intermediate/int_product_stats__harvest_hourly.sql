{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}

WITH harvest_with_product AS (
  SELECT
    h.txn_timestamp,
    p.chain_id,
    p.product_address,
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
  WHERE
    h.txn_timestamp IS NOT NULL
    AND toDate(h.txn_timestamp) > '1970-01-01'
)

SELECT
  hp.chain_id,
  hp.product_address,
  toStartOfHour(hp.txn_timestamp) AS date_hour,
  sum(hp.call_fee) AS harvest_call_fee,
  toDecimal256(sum(hp.call_fee * hp.native_price), 20) AS harvest_call_fee_usd,
  sum(hp.gas_fee) AS gas_fee,
  toDecimal256(sum(hp.gas_fee * hp.native_price), 20) AS gas_fee_usd,
  sum(hp.platform_fee) AS platform_fee,
  toDecimal256(sum(hp.platform_fee * hp.native_price), 20) AS platform_fee_usd,
  sum(hp.strategist_fee) AS strategist_fee,
  toDecimal256(sum(hp.strategist_fee * hp.native_price), 20) AS strategist_fee_usd,
  sum(hp.harvest_amount) AS harvest_amount,
  toDecimal256(sum(hp.harvest_amount * hp.want_price), 20) AS harvest_amount_usd,
  avg(hp.native_price) AS avg_native_price,
  count(DISTINCT hp.txn_hash) AS harvest_txn_count,
  1 AS harvest_vault_count
FROM harvest_with_product hp
GROUP BY hp.chain_id, hp.product_address, toStartOfHour(hp.txn_timestamp)
