{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history'],
    order_by=['kind', 'chain', 'object_id'],
  )
}}

-- One row per contract. Filter this table for search, /inactive, /o/[objectId], and all
-- /stats numbers except the active-over-time series (that is events windows).
-- live = in_catalog. Empty/missing status counts as active.

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
  {{ beefy_history_retire_reason_group('o.retire_reason') }} AS retire_reason_group,
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
  l.clm_parent_address,
  l.is_retired,
  l.is_paused,
  l.first_active_at,
  l.last_active_end_at,
  if(
    l.first_active_at IS NULL,
    NULL,
    concat(toString(toYear(l.first_active_at)), '-Q', toString(toQuarter(l.first_active_at)))
  ) AS launch_quarter,
  if(
    NOT l.is_retired OR l.last_active_end_at IS NULL,
    NULL,
    concat(toString(toYear(l.last_active_end_at)), '-Q', toString(toQuarter(l.last_active_end_at)))
  ) AS retirement_quarter,
  if(
    l.is_retired AND l.first_active_at IS NOT NULL AND l.last_active_end_at IS NOT NULL,
    toUInt32(
      intDiv(
        toUnixTimestamp(l.last_active_end_at) - toUnixTimestamp(l.first_active_at),
        {{ beefy_history_month_seconds() }}
      )
    ),
    NULL
  ) AS lifespan_months
FROM {{ ref('stg_beefy_history__objects') }} o
LEFT JOIN {{ ref('int_beefy_history__vault_lifecycle') }} l
  ON o.object_id = l.object_id
