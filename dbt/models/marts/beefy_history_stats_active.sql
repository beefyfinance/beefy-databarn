{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history', 'stats'],
    order_by=['sampled_at', 'chain', 'vault_type'],
  )
}}

-- Active counted vaults at each /stats sample, by chain and type.
-- SUM by chain → activeByChain; SUM by vault_type → activeByType. Missing combos are 0.

WITH counted AS (
  SELECT object_id, chain, vault_type
  FROM {{ ref('int_beefy_history__vault_lifecycle') }}
  WHERE counts_for_stats
),
counted_windows AS (
  SELECT
    w.committed_at AS valid_from_unix,
    w.valid_to_unix,
    c.chain,
    c.vault_type
  FROM {{ ref('int_beefy_history__event_windows') }} w
  INNER JOIN counted c
    ON c.object_id = w.object_id
  WHERE w.is_active
),
grid AS (
  SELECT
    s.sampled_at,
    s.sampled_at_ts,
    d.chain,
    d.vault_type
  FROM {{ ref('int_beefy_history__stats_samples') }} s
  CROSS JOIN (
    SELECT DISTINCT chain, vault_type
    FROM counted
  ) d
),
active AS (
  SELECT
    s.sampled_at,
    w.chain,
    w.vault_type,
    count() AS active_count
  FROM {{ ref('int_beefy_history__stats_samples') }} s
  INNER JOIN counted_windows w
    ON w.valid_from_unix <= s.sampled_at
    AND (w.valid_to_unix IS NULL OR s.sampled_at < w.valid_to_unix)
  GROUP BY
    s.sampled_at,
    w.chain,
    w.vault_type
)
SELECT
  g.sampled_at,
  g.sampled_at_ts,
  g.chain,
  g.vault_type,
  toUInt64(ifNull(a.active_count, 0)) AS active_count
FROM grid g
LEFT JOIN active a
  ON g.sampled_at = a.sampled_at
  AND g.chain = a.chain
  AND g.vault_type = a.vault_type
