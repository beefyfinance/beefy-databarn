{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history'],
    order_by=['seq'],
  )
}}

-- One row per catalog change. Filter for /changes, /o/[objectId] timeline, /commits/[repo]/[sha],
-- and /stats active-over-time (counts_for_stats AND is_active AND valid_from_unix <= T < valid_to).
-- prev_data is the previous non-null snapshot of the same object (changed events only).

SELECT
  e.seq,
  e.object_id,
  e.kind,
  e.chain,
  e.address,
  e.beefy_id,
  o.name,
  e.change_type,
  e.reason,
  e.source,
  e.commit_repo,
  e.commit_sha,
  e.committed_at,
  e.committed_at_ts,
  e.authored_at,
  e.authored_at_ts,
  e.commit_subject,
  concat('https://github.com/beefyfinance/', e.commit_repo, '/commit/', e.commit_sha) AS commit_url,
  if(
    e.path IS NULL OR e.source != e.commit_repo,
    NULL,
    concat('https://github.com/beefyfinance/', e.commit_repo, '/blob/', e.commit_sha, '/', e.path)
  ) AS file_url,
  e.path,
  {{ beefy_history_config_layer('e.path') }} AS config_layer,
  e.changed_keys,
  e.status,
  w.vault_type,
  e.platform_id,
  e.retire_reason,
  w.in_catalog,
  w.is_active,
  e.committed_at AS valid_from_unix,
  w.valid_from,
  w.valid_to_unix,
  w.valid_to,
  l.counts_for_stats,
  l.platform AS stats_platform,
  if(
    e.change_type = 'changed',
    anyLastIf(e.data, e.data IS NOT NULL) OVER (
      PARTITION BY e.object_id
      ORDER BY e.seq
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),
    NULL
  ) AS prev_data,
  e.data
FROM {{ ref('stg_beefy_history__events') }} e
LEFT JOIN {{ ref('int_beefy_history__event_windows') }} w
  ON e.object_id = w.object_id
  AND e.seq = w.seq
LEFT JOIN {{ ref('stg_beefy_history__objects') }} o
  ON e.object_id = o.object_id
LEFT JOIN {{ ref('int_beefy_history__vault_lifecycle') }} l
  ON e.object_id = l.object_id
