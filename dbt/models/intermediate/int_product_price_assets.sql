{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['asset_id', 'chain_id', 'product_address'],
    tags=['intermediate', 'prices'],
  )
}}

-- The price series (assets) that price a product's stats: its share token, its want, its LP oracle (the oracle keyed
-- by the product's beefy key, priced in /lps/breakdown), its chain's native token (harvest fees) and frxETH for
-- Fraxtal platform fees. A dirty asset of asset_price_5m reprices the stats of these products
-- (price_reprice(..., grain='product'), macros/prices/price_runs.sql).

WITH oracles AS (
  SELECT oracle_id, {{ asset_id_from_beefy_price_oracle_id('id') }} AS asset_id
  FROM {{ ref('stg_beefy_db__price_oracles') }}
)

SELECT DISTINCT chain_id, product_address, asset_id, role
FROM (
  SELECT t.chain_id AS chain_id, t.representation_address AS product_address, assumeNotNull(t.asset_id) AS asset_id, 'share' AS role
  FROM {{ ref('token') }} t
  INNER JOIN {{ ref('int_product_keys') }} pk ON t.chain_id = pk.chain_id AND t.representation_address = pk.product_address
  WHERE t.asset_id IS NOT NULL

  UNION ALL
  SELECT chain_id, product_address, {{ asset_id_from_beefy_price_oracle_id('beefy_wants_price_oracle_id') }}, 'want'
  FROM {{ ref('int_product_keys') }}
  WHERE beefy_wants_price_oracle_id IS NOT NULL AND beefy_wants_price_oracle_id != ''

  UNION ALL
  SELECT pk.chain_id, pk.product_address, o.asset_id, 'lp'
  FROM {{ ref('int_product_keys') }} pk
  INNER JOIN oracles o ON pk.beefy_key = o.oracle_id

  UNION ALL
  SELECT pk.chain_id, pk.product_address, o.asset_id, 'native'
  FROM {{ ref('int_product_keys') }} pk
  INNER JOIN {{ ref('int_chain_keys') }} c ON pk.chain_id = c.chain_id
  INNER JOIN oracles o ON c.beefy_native_price_oracle_key = o.oracle_id

  UNION ALL
  SELECT pk.chain_id, pk.product_address, o.asset_id, 'platform_fee'
  FROM {{ ref('int_product_keys') }} pk
  INNER JOIN oracles o ON o.oracle_id = 'frxETH'
  WHERE pk.chain_id = 252 -- fraxtal
)
