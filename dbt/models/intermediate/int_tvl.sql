{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

SELECT
  p.chain_id,
  p.product_address,
  t.date_time,
  t.tvl
FROM {{ ref('stg_beefy_api__tvl') }} t
INNER JOIN {{ ref('int_product_keys') }} p
  ON t.network_id = p.chain_id
  AND t.vault_id = p.beefy_key