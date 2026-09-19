{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.id as String) as id,
  cast(t.oracle_id as String) as oracle_id,
  ifNull(t.tokens, '[]') as tokens
FROM {{ source('dlt', 'beefy_db___price_oracles') }} t FINAL

