{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  ifNull(t.name, 'Unknown') as name,
  ifNull(t.beefy_name, 'Unknown') as beefy_name,
  toBool(t.enabled != 0) as enabled
FROM {{ source('dlt', 'beefy_db___chains') }} t FINAL

