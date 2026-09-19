{{
  config(
    materialized='view',
  )
}}

SELECT
  t.token_decimals,
  t.token_price,
  t.usd_value,
  cast(t.etag as String) as etag,
  cast(t.chain_id as String) as chain_id,
  cast(t.wallet_address as String) as wallet_address,
  t.wallet_name,
  {{ evm_address('t.token_address') }} as token_address,
  cast(t.date_time as DateTime('UTC')) as date_time,
  t.name,
  t.address,
  t.decimals,
  t.asset_type,
  t.oracle_id,
  t.oracle_type,
  t.symbol,
  t.price,
  t.balance,
  t.id,
  toBool(t.staked) as staked,
  t.number_id,
  t.method,
  t.method_path,
  t.vault_id,
  t.price_per_full_share,
  t.helper
FROM {{ source('dlt', 'beefy_api___treasury') }} t FINAL

