{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history', 'stats'],
    order_by=['quarter_start', 'series', 'series_key'],
  )
}}

-- /stats quarterly launches and retirements, long form.
-- series: launched_type | launched_chain | retired_reason. Only non-zero counts.
-- Left-join beefy_history_stats_quarter_index for empty quarters on the chart axis.

WITH counted AS (
  SELECT
    chain,
    vault_type,
    first_active_at,
    last_active_end_at,
    is_retired,
    ifNull(retire_reason, '') AS retire_reason
  FROM {{ ref('int_beefy_history__vault_lifecycle') }}
  WHERE counts_for_stats
),
launched_type AS (
  SELECT
    concat(toString(toYear(first_active_at)), '-Q', toString(toQuarter(first_active_at))) AS quarter,
    toUnixTimestamp(toStartOfQuarter(first_active_at)) AS quarter_start,
    'launched_type' AS series,
    vault_type AS series_key,
    CAST(NULL AS Nullable(String)) AS reason_group,
    toUInt64(count()) AS vault_count
  FROM counted
  GROUP BY
    quarter,
    quarter_start,
    series_key
),
launched_chain AS (
  SELECT
    concat(toString(toYear(first_active_at)), '-Q', toString(toQuarter(first_active_at))) AS quarter,
    toUnixTimestamp(toStartOfQuarter(first_active_at)) AS quarter_start,
    'launched_chain' AS series,
    chain AS series_key,
    CAST(NULL AS Nullable(String)) AS reason_group,
    toUInt64(count()) AS vault_count
  FROM counted
  GROUP BY
    quarter,
    quarter_start,
    series_key
),
retired AS (
  SELECT
    concat(toString(toYear(last_active_end_at)), '-Q', toString(toQuarter(last_active_end_at))) AS quarter,
    toUnixTimestamp(toStartOfQuarter(last_active_end_at)) AS quarter_start,
    'retired_reason' AS series,
    retire_reason AS series_key,
    {{ beefy_history_retire_reason_group('retire_reason') }} AS reason_group,
    toUInt64(count()) AS vault_count
  FROM counted
  WHERE is_retired
  GROUP BY
    quarter,
    quarter_start,
    series_key,
    reason_group
)
SELECT * FROM launched_type
UNION ALL
SELECT * FROM launched_chain
UNION ALL
SELECT * FROM retired
