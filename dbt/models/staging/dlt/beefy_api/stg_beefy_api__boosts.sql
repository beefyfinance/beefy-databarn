{{
  config(
    materialized='view',
  )
}}

SELECT
  t.assets,
  t.partners,
  t.period_finishes,
  cast(t.id as String) as id,
  t.name,
  t.chain,
  t.pool_id,
  t.version,
  t.status,
  cast({{ evm_address('t.earn_contract_address') }} as String) as boost_contract_address,
  {{ evm_address('t.token_address') }} as underlying_token_address,
  t.earned_token,
  t.earned_token_decimals,
  cast({{ evm_address('t.earned_token_address') }} as String) as reward_token_address,
  t.earned_oracle,
  t.earned_oracle_id,
  t.partnership,
  toBool(t.is_moo_staked) as is_moo_staked,
  t.period_finish,
  t.campaign
FROM {{ source('dlt', 'beefy_api___boosts') }} t FINAL

