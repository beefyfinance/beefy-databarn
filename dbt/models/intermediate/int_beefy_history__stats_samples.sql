{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['sampled_at'],
  )
}}

-- /stats `at` instants: every Monday and every 1st of a month (00:00 UTC) from monday_start
-- through stats_end, then stats_end unless it is already one of them. Month-starts begin at
-- the first of the month after monday_start's month (same as packages/queries/src/stats.ts).

WITH bounds AS (
  SELECT first_launch, stats_end, monday_start
  FROM {{ ref('int_beefy_history__stats_bounds') }}
),
mondays AS (
  SELECT
    b.monday_start + n * 604800 AS sampled_at
  FROM bounds b
  ARRAY JOIN range(
    toUInt64(intDiv(greatest(b.stats_end, b.monday_start) - b.monday_start, 604800) + 1)
  ) AS n
  WHERE b.monday_start + n * 604800 <= b.stats_end
),
month_starts AS (
  SELECT ts AS sampled_at
  FROM (
    SELECT
      toUnixTimestamp(
        toStartOfMonth(addMonths(toDateTime(b.monday_start, 'UTC'), toUInt32(n) + 1))
      ) AS ts,
      b.stats_end AS stats_end
    FROM bounds b
    ARRAY JOIN range(
      toUInt64(
        dateDiff('month', toDateTime(b.monday_start, 'UTC'), toDateTime(b.stats_end, 'UTC')) + 3
      )
    ) AS n
  )
  WHERE ts <= stats_end
),
all_samples AS (
  SELECT sampled_at FROM mondays
  UNION DISTINCT
  SELECT sampled_at FROM month_starts
  UNION DISTINCT
  SELECT stats_end AS sampled_at FROM bounds
)
SELECT
  s.sampled_at,
  toDateTime64(s.sampled_at, 0, 'UTC') AS sampled_at_ts,
  s.sampled_at = b.stats_end AS is_end,
  (s.sampled_at - 345600) % 604800 = 0 AS is_monday,
  toDayOfMonth(toDateTime(s.sampled_at, 'UTC')) = 1
    AND s.sampled_at % 86400 = 0 AS is_month_start
FROM all_samples s
CROSS JOIN bounds b
