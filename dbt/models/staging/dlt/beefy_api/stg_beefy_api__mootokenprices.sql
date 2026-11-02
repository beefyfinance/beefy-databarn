{{
  config(
    materialized='view',
  )
}}

-- price is per 1e18 raw share units. Before var mootoken_price_fix_deployed_at, /mootokenprices divided by 1e18
-- instead of 10^want_decimals (beefy-api PR 1966), so older snapshots are rescaled here.

SELECT
  cast(t.etag as String) as etag,
  cast(t.chain_id as String) as chain_id,
  ifNull(t.moo_token_symbol, 'Unknown') as moo_token_symbol,
  toFloat64(t.price) * if(
    t.date_time < toDateTime('{{ var("mootoken_price_fix_deployed_at") }}', 'UTC'),
    pow(10, 18 - ifNull(v.token_decimals, 18)),
    1
  ) as price,
  cast(t.date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api___mootokenprices') }} t FINAL
LEFT JOIN {{ ref('stg_beefy_api__vaults') }} v
  ON {{ normalize_network_beefy_key('t.chain_id') }} = v.network
  AND t.moo_token_symbol = v.earned_token
where t.price is not null
