{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history'],
    order_by=['kind', 'chain', 'object_id'],
  )
}}

-- Search, inactive list, and object header for history.beefy.rodeo.
-- live = in_catalog. is_inactive matches /inactive: live with a non-active status, or removed.
-- is_currently_active: in catalog with missing/empty/active status (empty counts as active).

SELECT
  o.object_id,
  o.kind,
  o.chain,
  o.address,
  o.beefy_id,
  o.beefy_ids,
  o.name,
  o.title,
  o.vault_type,
  o.status,
  o.assets,
  o.platform_id,
  o.token_provider_id,
  o.token_address,
  o.earned_token_address,
  o.earned_token_addresses,
  o.earn_contract_address,
  o.retire_reason,
  o.in_catalog AS live,
  o.first_committed_at,
  o.first_committed_at_ts,
  o.last_committed_at,
  o.last_committed_at_ts,
  if(o.in_catalog, NULL, o.last_committed_at) AS removed_at,
  o.last_change_type,
  o.event_count,
  o.source,
  o.path,
  o.config_layer,
  o.data,
  o.in_catalog AND (o.status IS NULL OR o.status = 'active') AS is_currently_active,
  (o.in_catalog AND o.status IS NOT NULL AND o.status != 'active')
    OR NOT o.in_catalog AS is_inactive,
  l.platform AS stats_platform,
  l.counts_for_stats,
  l.is_clm_wrapper,
  l.is_retired,
  l.is_paused,
  l.first_active_at,
  l.last_active_end_at
FROM {{ ref('stg_beefy_history__objects') }} o
LEFT JOIN {{ ref('int_beefy_history__vault_lifecycle') }} l
  ON o.object_id = l.object_id
