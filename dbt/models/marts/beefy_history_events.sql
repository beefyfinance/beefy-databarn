{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history'],
    order_by=['seq'],
  )
}}

-- Change feed, object timeline, and commit pages. prev_data is the previous non-null snapshot
-- of the same object (only filled for change_type = changed).

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
  {{ beefy_history_vault_type('e.config_type', 'e.is_gov_vault') }} AS vault_type,
  e.platform_id,
  e.retire_reason,
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
LEFT JOIN {{ ref('stg_beefy_history__objects') }} o
  ON e.object_id = o.object_id
