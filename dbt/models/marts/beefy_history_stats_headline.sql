{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history', 'stats'],
    order_by=['stats_end'],
  )
}}

-- Headline numbers for /stats (StatTiles). One row. Empty when no counted vault exists.

WITH chain_totals AS (
  SELECT
    sampled_at,
    chain,
    sum(active_count) AS n
  FROM {{ ref('beefy_history_stats_active') }}
  GROUP BY
    sampled_at,
    chain
),
totals AS (
  SELECT
    sampled_at,
    sum(n) AS active_total,
    countIf(n > 0) AS chains_active
  FROM chain_totals
  GROUP BY sampled_at
),
last_sample AS (
  SELECT max(sampled_at) AS sampled_at
  FROM totals
),
now AS (
  SELECT t.active_total, t.chains_active
  FROM totals t
  INNER JOIN last_sample l ON t.sampled_at = l.sampled_at
),
peak AS (
  SELECT
    sampled_at AS peak_at,
    active_total AS peak
  FROM totals
  ORDER BY active_total DESC, sampled_at ASC
  LIMIT 1
),
lifespan AS (
  SELECT
    months,
    vault_count,
    sum(vault_count) OVER (ORDER BY months) AS running
  FROM {{ ref('beefy_history_stats_lifespan') }}
),
lifespan_total AS (
  SELECT sum(vault_count) AS total
  FROM {{ ref('beefy_history_stats_lifespan') }}
),
median AS (
  SELECT min(l.months) AS median_lifespan_months
  FROM lifespan l
  CROSS JOIN lifespan_total t
  WHERE t.total > 0
    AND l.running > intDiv(t.total - 1, 2)
)
SELECT
  b.stats_end,
  toDateTime64(b.stats_end, 0, 'UTC') AS stats_end_ts,
  b.first_launch,
  toUInt64(ifNull(n.active_total, 0)) AS active_now,
  toUInt64(ifNull(p.peak, 0)) AS peak,
  p.peak_at,
  toUInt64(ifNull(n.chains_active, 0)) AS chains_now,
  (
    SELECT toUInt64(uniqExact(chain))
    FROM {{ ref('beefy_history_stats_active') }}
  ) AS chains_ever,
  (
    SELECT toUInt64(sum(vault_count))
    FROM {{ ref('beefy_history_stats_quarters') }}
    WHERE series = 'launched_type'
  ) AS launched,
  (
    SELECT toUInt64(sum(vault_count))
    FROM {{ ref('beefy_history_stats_quarters') }}
    WHERE series = 'retired_reason'
  ) AS retired,
  m.median_lifespan_months
FROM {{ ref('int_beefy_history__stats_bounds') }} b
LEFT JOIN now n ON 1 = 1
LEFT JOIN peak p ON 1 = 1
LEFT JOIN median m ON 1 = 1
