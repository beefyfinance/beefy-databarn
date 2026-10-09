{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['first_launch'],
  )
}}

-- One row: /stats sample window. first_launch is the first in-catalog active observation among
-- counted vaults (CLM wrappers excluded). stats_end is the latest event committer time.

WITH counted AS (
  SELECT first_active_at
  FROM {{ ref('int_beefy_history__vault_lifecycle') }}
  WHERE counts_for_stats
),
agg AS (
  SELECT
    count() AS counted_vaults,
    toUnixTimestamp(min(first_active_at)) AS first_launch,
    greatest(
      toUnixTimestamp(min(first_active_at)),
      (SELECT max(committed_at) FROM {{ ref('stg_beefy_history__events') }})
    ) AS stats_end
  FROM counted
)
SELECT
  counted_vaults,
  first_launch,
  stats_end,
  -- Monday 00:00 UTC on or before first_launch (1970-01-05 is unix Monday 0).
  first_launch - modulo(modulo(toInt64(first_launch) - 345600, 604800) + 604800, 604800) AS monday_start
FROM agg
WHERE counted_vaults > 0
