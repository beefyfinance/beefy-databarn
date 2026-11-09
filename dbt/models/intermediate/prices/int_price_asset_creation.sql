{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['asset_id'],
    tags=['intermediate', 'prices'],
  )
}}

-- When each product share asset came into existence: the creation of its classic vault or CLM manager (envio, else
-- databarn v1), or the API's declared creation date when neither is known. fact_asset_price_5m carries an asset's
-- first value back to this time when it comes at most var price_backfill_days later (price_backfill in
-- macros/prices/price_resolver_sql.sql): first deposits are made minutes after deployment, before any source prices
-- the share. Only share tokens of a vault or a CLM: an oracle that other tokens map to (a reward pool's underlying)
-- has a history of its own.

SELECT
  assumeNotNull(t.asset_id) AS asset_id,
  toDateTime(min(coalesce(p.creation_datetime, p.declared_creation_date)), 'UTC') AS created_at
FROM {{ ref('dim_product') }} p
INNER JOIN {{ ref('dim_token') }} t
  ON t.chain_id = p.chain_id AND t.representation_address = p.product_address
WHERE p.product_type IN ('classic_vault', 'clm_manager')
  AND t.token_kind IN ('beefy_classic_vault', 'beefy_clm_manager')
  AND t.asset_id IS NOT NULL
  AND coalesce(p.creation_datetime, p.declared_creation_date) IS NOT NULL
GROUP BY t.asset_id
