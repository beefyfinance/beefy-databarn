{{
  config(
    materialized='view',
  )
}}

SELECT
  t.chain_key AS chain_key,
  cast(t.loaded_at as DateTime('UTC')) AS loaded_at,
  t.deployment AS deployment,
  t.has_indexing_errors AS has_indexing_errors,
  cast(t.head_block_number as Int64) AS head_block_number,
  cast(t.head_block_timestamp as Nullable(DateTime('UTC'))) AS head_block_timestamp
FROM {{ source('dlt', 'beefy_clm_subgraph___subgraph_status') }} t
