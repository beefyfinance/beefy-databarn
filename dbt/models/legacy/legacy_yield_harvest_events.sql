{{ legacy_proxy_guard() }}
SELECT
  h.txn_timestamp AS txn_timestamp,
  h.chain_id AS chain_id,
  c.chain_name AS chain_name,
  h.product_address AS product_address,
  h.block_number AS block_number,
  h.txn_idx AS txn_idx,
  h.event_idx AS event_idx,
  h.txn_hash AS txn_hash,
  h.harvest_amount AS underlying_amount_compounded,
  h.want_price AS underlying_token_price_usd,
  h.harvest_amount_usd AS underlying_amount_compounded_usd
FROM {{ ref('fact_harvest_obs') }} h
INNER JOIN {{ ref('dim_chain') }} c ON h.chain_id = c.chain_id
