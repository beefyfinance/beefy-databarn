{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history', 'stats'],
    order_by=['months'],
  )
}}

-- Retired counted vaults by whole months from launch to retirement (30.4375-day months).
-- Index `months` is the API's lifespanMonths[n]. Gaps are filled with 0 through the max.

WITH retired AS (
  SELECT
    toUInt32(
      intDiv(
        toUnixTimestamp(last_active_end_at) - toUnixTimestamp(first_active_at),
        {{ beefy_history_month_seconds() }}
      )
    ) AS months
  FROM {{ ref('int_beefy_history__vault_lifecycle') }}
  WHERE counts_for_stats
    AND is_retired
    AND first_active_at IS NOT NULL
    AND last_active_end_at IS NOT NULL
),
hist AS (
  SELECT
    months,
    toUInt64(count()) AS vault_count
  FROM retired
  GROUP BY months
),
max_m AS (
  SELECT
    max(months) AS max_months,
    count() AS n
  FROM hist
)
SELECT
  toUInt32(month_idx) AS months,
  toUInt64(ifNull(h.vault_count, 0)) AS vault_count
FROM max_m
ARRAY JOIN range(if(n = 0, toUInt64(0), toUInt64(max_months) + 1)) AS month_idx
LEFT JOIN hist h
  ON h.months = toUInt32(month_idx)
