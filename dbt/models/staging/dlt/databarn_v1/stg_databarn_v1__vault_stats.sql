{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.datetime as DateTime('UTC')) AS datetime,
  cast(t.product_id as Int64) AS product_id,
  cast(t.block_number as Int64) AS block_number,
  {{ to_decimal('t.vault_total_supply') }} AS vault_total_supply,
  {{ to_decimal('t.share_to_underlying_price') }} AS share_to_underlying_price,
  {{ to_decimal('t.staked_underlying') }} AS staked_underlying,
  {{ to_decimal('t.underlying_total_supply') }} AS underlying_total_supply,
  {{ to_float('t.underlying_capture_percentage') }} AS underlying_capture_percentage
FROM {{ source('dlt', 'databarn_v1___vault_stats') }} t FINAL
