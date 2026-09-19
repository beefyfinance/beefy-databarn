{{
  config(
    materialized='view',
  )
}}

SELECT
  t.assets,
  t.risks,
  t.point_structure_ids,
  t.deposit_token_addresses,
  t.zaps,
  cast(t.id as String) as id,
  t.name,
  t.token,
  t.token_address,
  t.token_decimals,
  t.token_provider_id,
  t.earned_token,
  t.earned_token_address,
  t.earn_contract_address,
  t.oracle,
  t.oracle_id,
  t.status,
  t.created_at,
  t.platform_id,
  t.strategy_type_id,
  t.network,
  t.type,
  t.fee_tier,
  toBool(ifNull(t.is_gov_vault, false)) as is_gov_vault,
  t.chain,
  t.strategy,
  t.last_harvest,
  t.retire_reason,
  t.retired_at,
  toBool(t.earning_points) as earning_points,
  t.updated_at
FROM {{ source('dlt', 'beefy_api___cow_vaults') }} t FINAL

