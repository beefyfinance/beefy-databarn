{{
  config(
    materialized='view',
    tags=['marts', 'zap'],
  )
}}

-- Mart: Zap (cross-chain bridge) events with source and target chain dimension context
SELECT
  z.network_id as chain_id,
  src.chain_name as source_chain_name,
  src.beefy_key as source_chain_beefy_key,
  src.beefy_enabled as source_chain_beefy_enabled,
  z.block_number,
  z.txn_idx,
  z.event_idx,
  z.txn_timestamp,
  z.txn_hash,
  z.caller_address,
  z.recipient_address,
  z.target_chain_id,
  tgt.chain_name as target_chain_name,
  tgt.beefy_key as target_chain_beefy_key,
  tgt.beefy_enabled as target_chain_beefy_enabled,
  z.vault_id,
  z.action,
  z.total_usd,
  z.updated_at
FROM {{ ref('stg_beefy_db__zap_events') }} z
LEFT JOIN {{ ref('chain') }} src
  ON z.network_id = src.chain_id
LEFT JOIN {{ ref('chain') }} tgt
  ON z.target_chain_id = tgt.chain_id
WHERE z.action <> 'swap' -- fron: the swap actions are not related to beefy