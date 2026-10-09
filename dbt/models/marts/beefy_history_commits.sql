{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'beefy_history'],
    order_by=['commit_repo', 'commit_sha'],
  )
}}

-- One row per git commit that produced catalog events (/commits/[repo]/[sha] index).

SELECT
  commit_repo,
  commit_sha,
  any(commit_subject) AS commit_subject,
  min(committed_at) AS committed_at,
  min(committed_at_ts) AS committed_at_ts,
  concat('https://github.com/beefyfinance/', commit_repo, '/commit/', commit_sha) AS commit_url,
  count() AS event_count,
  uniqExact(object_id) AS object_count,
  countIf(change_type IN ('added', 'readded')) AS added_count,
  countIf(change_type = 'changed') AS changed_count,
  countIf(change_type = 'removed') AS removed_count
FROM {{ ref('stg_beefy_history__events') }}
GROUP BY
  commit_repo,
  commit_sha
