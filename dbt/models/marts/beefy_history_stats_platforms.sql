{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history', 'stats'],
    order_by=['platform'],
  )
}}

-- Counted vaults per platform (platformId, or tokenProviderId for CLM). '' = none.
-- active_now is still active at stats_end (last active period has not ended).

SELECT
  ifNull(platform, '') AS platform,
  toUInt64(count()) AS launched,
  toUInt64(countIf(last_active_end_at IS NULL)) AS active_now
FROM {{ ref('int_beefy_history__vault_lifecycle') }}
WHERE counts_for_stats
GROUP BY
  ifNull(platform, '')
