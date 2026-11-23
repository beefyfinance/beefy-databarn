{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(ci.id as Int64) as chain_id,
  ci.chain_id as chain_key
FROM {{ source('dlt', 'beefy_db___chain_ids') }} ci FINAL
