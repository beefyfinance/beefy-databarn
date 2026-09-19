{{
  config(
    materialized='view',
  )
}}

SELECT
  toFloat64(t.apy) as apy,
  cast(t.etag as String) as etag,
  cast(t.vault_id as String) as vault_id,
  cast(t.date_time as DateTime('UTC')) as date_time,
  t.apy__v_text
FROM {{ source('dlt', 'beefy_api___apy') }} t FINAL
where t.apy is not null
