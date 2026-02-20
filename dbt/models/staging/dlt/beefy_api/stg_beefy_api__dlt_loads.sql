{{
  config(
    materialized='view',
  )
}}

SELECT
  load_id,
  schema_name,
  status,
  inserted_at,
  schema_version_hash
FROM {{ source('dlt', 'beefy_api____dlt_loads') }}
