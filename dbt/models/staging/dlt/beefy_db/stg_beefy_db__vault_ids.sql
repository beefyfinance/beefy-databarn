{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(vi.id as String) as vault_id,
  vi.vault_id as beefy_key
FROM {{ source('dlt', 'beefy_db___vault_ids') }} vi FINAL

