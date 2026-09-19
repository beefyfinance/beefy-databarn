{{
  config(
    materialized='view',
  )
}}

SELECT
  t.type,
  cast(t.id as String) as id,
  ifNull(t.symbol, 'Unknown') as symbol,
  ifNull(t.name, 'Unknown') as name,
  {{ normalize_network_beefy_key('t.chain_id') }} as chain_beefy_key,
  t.oracle,
  t.oracle_id,
  cast({{ evm_address('t.address') }} as String) as address,
  cast(t.decimals as Int64) as decimals,
  t.bridge,
  toBool(t.staked) as staked
FROM {{ source('dlt', 'beefy_api___tokens') }} t FINAL
where t.chain_id is not null
