{{
  config(
    materialized='table',
    tags=['intermediate', 'yield'],
    order_by=['txn_timestamp', 'chain_id', 'product_address'],
    engine='MergeTree',
  )
}}

-- One row per harvest of a listed product: the want compounded and its USD value (prices from int_harvests,
-- asset_price_5m on the harvest's slot). A table: rebuilt from int_harvests each run, so re-resolved prices reach
-- every harvest.

SELECT
  h.txn_timestamp as txn_timestamp,
  p.chain_id,
  p.product_address,
  h.block_number,
  h.txn_idx,
  h.event_idx,
  h.txn_hash as txn_hash,
  h.harvest_amount as underlying_amount_compounded,
  h.want_price as underlying_token_price_usd,
  toDecimal256(h.harvest_amount * h.want_price, 20) as underlying_amount_compounded_usd
FROM {{ ref('int_harvests') }} h
INNER JOIN {{ ref('int_product_keys') }} p
  ON h.network_id = p.chain_id
  AND h.vault_beefy_key = p.beefy_key
WHERE
  h.txn_timestamp IS NOT NULL
  AND toDate(h.txn_timestamp) > '1970-01-01'
