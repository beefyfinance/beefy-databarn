{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.oracle_id as String) as oracle_id,
  cast(t.t as DateTime('UTC')) as t,
  {{ to_decimal('t.val') }} as val
FROM {{ source('dlt', 'beefy_db___prices') }} t FINAL
