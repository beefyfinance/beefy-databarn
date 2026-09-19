{{
  config(
    materialized='view',
    order_by=['date_time', 'vault_id'],
  )
}}

SELECT
  cast(t.etag as String) as etag,
  cast(t.vault_id as String) as vault_id,
  cast(t.date_time as DateTime('UTC')) as date_time,
  toFloat64(t.price) as price,
  {{ to_representation_evm_address_list('t.tokens') }} as tokens,
  {{ to_decimal_list('t.balances') }} as balances,
  {{ to_decimal('t.total_supply') }} as total_supply,
  {{ to_decimal('t.underlying_liquidity') }} as underlying_liquidity,
  {{ to_decimal_list('t.underlying_balances') }} as underlying_balances,
  {{ to_decimal('t.underlying_price') }} as underlying_price
FROM {{ source('dlt', 'beefy_api___lps_breakdown') }} t FINAL

