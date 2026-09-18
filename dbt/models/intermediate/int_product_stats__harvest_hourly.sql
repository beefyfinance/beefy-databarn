{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}

-- Fraxtal (252) platform fees are paid in frxETH, not native FRAX.
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
  INNER JOIN {{ ref('int_product_keys') }} p
    ON h.network_id = p.chain_id
    AND h.vault_beefy_key = p.beefy_key
  WHERE
    h.txn_timestamp IS NOT NULL
    AND toDate(h.txn_timestamp) > '1970-01-01'
),
frxeth_price AS (
  SELECT
    toInt64(252) AS chain_id, -- fraxtal
    date_time,
    price
  FROM {{ ref('int_price') }}
  WHERE oracle_key = 'frxETH'
),
harvest_priced AS (
  SELECT
    hp.txn_timestamp,
    hp.chain_id,
    hp.product_address,
    hp.txn_hash,
    hp.call_fee,
    hp.gas_fee,
    hp.platform_fee,
    hp.strategist_fee,
    hp.harvest_amount,
    hp.native_price,
    hp.want_price,
    coalesce(frx.price, hp.native_price) AS platform_fee_price
  FROM harvest_with_product hp
  ASOF LEFT JOIN frxeth_price frx
    ON hp.chain_id = frx.chain_id
    AND hp.txn_timestamp >= frx.date_time
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
  toDecimal256(sum(hp.platform_fee * hp.platform_fee_price), 20) AS platform_fee_usd,
  sum(hp.strategist_fee) AS strategist_fee,
  toDecimal256(sum(hp.strategist_fee * hp.native_price), 20) AS strategist_fee_usd,
  sum(hp.harvest_amount) AS harvest_amount,
  toDecimal256(sum(hp.harvest_amount * hp.want_price), 20) AS harvest_amount_usd,
  avg(hp.native_price) AS avg_native_price,
  count(DISTINCT hp.txn_hash) AS harvest_txn_count,
  1 AS harvest_vault_count
FROM harvest_priced hp
GROUP BY hp.chain_id, hp.product_address, toStartOfHour(hp.txn_timestamp)
