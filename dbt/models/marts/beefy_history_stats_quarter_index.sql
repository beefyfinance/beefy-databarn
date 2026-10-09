{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history', 'stats'],
    order_by=['quarter_start'],
  )
}}

-- Every UTC quarter from the first counted launch through stats_end (chart x-axis, including empty ones).

WITH bounds AS (
  SELECT first_launch, stats_end
  FROM {{ ref('int_beefy_history__stats_bounds') }}
)
SELECT
  concat(toString(toYear(q)), '-Q', toString(toQuarter(q))) AS quarter,
  toUnixTimestamp(q) AS quarter_start,
  q AS quarter_start_ts
FROM (
  SELECT
    addQuarters(toStartOfQuarter(toDateTime(b.first_launch, 'UTC')), toUInt32(n)) AS q
  FROM bounds b
  ARRAY JOIN range(
    toUInt64(
      dateDiff(
        'quarter',
        toStartOfQuarter(toDateTime(b.first_launch, 'UTC')),
        toStartOfQuarter(toDateTime(b.stats_end, 'UTC'))
      ) + 1
    )
  ) AS n
)
