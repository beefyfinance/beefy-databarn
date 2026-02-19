{{
  config(
    materialized='view',
    tags=['marts', 'zap'],
  )
}}

-- Mart: Zap token transfers (inputs/outputs/refunds) with chain, token and parent zap event context
SELECT
  zt.network_id as chain_id,
  c.chain_name as chain_name,
  c.beefy_key as chain_beefy_key,
  zt.block_number as block_number,
  zt.txn_idx as txn_idx,
  zt.parent_event_idx as parent_event_idx,
  zt.event_idx as event_idx,
  zt.zap_type as zap_type,
  zt.token_amount as token_amount,
  zt.usd_value as usd_value,
  zt.token_address as token_address,
  po.oracle_id as raw_token_symbol, -- todo: use the "token" dimension, but that can be either a token or a product
  ze.txn_timestamp as txn_timestamp,
  ze.txn_hash as txn_hash,
  ze.caller_address as caller_address,
  ze.recipient_address as recipient_address,
  ze.target_chain_id as target_chain_id,
  ze.vault_id as vault_id,
  ze.action as zap_action,
  zt.updated_at as updated_at
FROM {{ ref('stg_beefy_db__zap_token_transfers') }} zt
LEFT JOIN {{ ref('chain') }} c
  ON zt.network_id = c.chain_id
LEFT JOIN {{ ref('stg_beefy_db__price_oracles') }} po
  ON toString(zt.token_id) = po.id
LEFT JOIN {{ ref('stg_beefy_db__zap_events') }} ze
  ON zt.network_id = ze.network_id
  AND zt.block_number = ze.block_number
  AND zt.txn_idx = ze.txn_idx
  AND zt.parent_event_idx = ze.event_idx
WHERE ze.action <> 'swap' -- fron: the swap actions are not related to beefy