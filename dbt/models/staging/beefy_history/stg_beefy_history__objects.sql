{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['object_id'],
  )
}}

-- One row per contract (object_id = kind:chain:address). DuckDB `objects` is a view, not a parquet file.

WITH agg AS (
  SELECT
    object_id,
    argMax(kind, seq) AS kind,
    argMax(chain, seq) AS chain,
    argMax(address, seq) AS address,
    argMax(beefy_id, seq) AS beefy_id,
    arrayDistinct(
      arrayMap(
        x -> tupleElement(x, 1),
        arraySort(x -> tupleElement(x, 2), groupArray((beefy_id, seq)))
      )
    ) AS beefy_ids,
    min(seq) AS first_seq,
    max(seq) AS last_seq,
    argMin(committed_at, seq) AS first_committed_at,
    argMax(committed_at, seq) AS last_committed_at,
    argMax(change_type, seq) AS last_change_type,
    count() AS event_count,
    maxIf(seq, data IS NOT NULL) AS data_seq
  FROM {{ ref('stg_beefy_history__events') }}
  GROUP BY object_id
)
SELECT
  a.object_id,
  a.kind,
  a.chain,
  a.address,
  a.beefy_id,
  a.beefy_ids,
  a.first_seq,
  a.last_seq,
  a.first_committed_at,
  toDateTime64(a.first_committed_at, 0, 'UTC') AS first_committed_at_ts,
  a.last_committed_at,
  toDateTime64(a.last_committed_at, 0, 'UTC') AS last_committed_at_ts,
  a.last_change_type,
  a.last_change_type != 'removed' AS in_catalog,
  a.event_count,
  e.data,
  e.status,
  e.config_type,
  e.is_gov_vault,
  e.platform_id,
  e.token_provider_id,
  e.retire_reason,
  e.token_address,
  e.earn_contract_address,
  {{ beefy_history_vault_type('e.config_type', 'ifNull(e.is_gov_vault, false)') }} AS vault_type
FROM agg a
LEFT JOIN {{ ref('stg_beefy_history__events') }} e
  ON e.seq = a.data_seq
