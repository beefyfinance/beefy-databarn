{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}

WITH yield_with_product AS (
  SELECT
    a.txn_timestamp,
    a.chain_id,
    a.product_address,
    a.underlying_amount_compounded,
    a.underlying_token_price_usd,
    a.underlying_amount_compounded_usd
  FROM {{ ref('int_yield') }} a
  WHERE 
    a.underlying_amount_compounded between 0 and 1000000000000
    and a.underlying_token_price_usd between 0 and 1000000
    and a.underlying_amount_compounded_usd between 0 and 1000000000000
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(txn_timestamp) as date_hour,
  argMax(underlying_amount_compounded, txn_timestamp) as underlying_amount_compounded,
  argMax(underlying_token_price_usd, txn_timestamp) as underlying_token_price_usd,
  argMax(underlying_amount_compounded_usd, txn_timestamp) as underlying_amount_compounded_usd
FROM yield_with_product
GROUP BY chain_id, product_address, toStartOfHour(txn_timestamp)
