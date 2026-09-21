{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['id'],
  )
}}

-- Table, not a view: one copy of the latest completed load per dbt run.
-- product_clm and int_product_keys both read this table, so they cannot
-- diverge when a newer beefy_api load lands mid-run.
-- No FINAL: an in-flight load must not hide rows from the completed load_id.

WITH source AS (
  SELECT *
  FROM {{ source('dlt', 'beefy_api___clm_vaults') }}
  WHERE _dlt_load_id = {{ latest_dlt_load_id('beefy_api', 'clm_vaults') }}
  LIMIT 1 BY id
)

SELECT
  t.assets,
  t.fee_tier,
  t.risks,
  t.point_structure_ids,
  JSONExtract(ifNull(t.deposit_token_addresses, '[]'), 'Array(String)') as deposit_token_addresses,
  t.zaps,
  t.vault,
  t.pool,
  cast(t.id as String) as id,
  ifNull(t.name, 'Unknown') as name,
  t.token,
  cast({{ evm_address('t.token_address') }} as String) as token_address,
  t.token_decimals,
  t.token_provider_id,
  t.earned_token,
  cast({{ evm_address('t.earned_token_address') }} as String) as earned_token_address,
  cast({{ evm_address('t.earn_contract_address') }} as String) as earn_contract_address,
  t.oracle,
  t.oracle_id,
  t.status,
  t.created_at,
  cast(t.platform_id as String) as platform_id,
  t.strategy_type_id,
  {{ normalize_network_beefy_key('t.network') }} as network,
  t.type,
  toBool(ifNull(t.is_gov_vault, false)) as is_gov_vault,
  t.chain,
  {{ evm_address('t.strategy') }} as strategy,
  t.last_harvest,
  t.retire_reason,
  t.retired_at,
  toBool(t.earning_points) as earning_points,
  t.updated_at
FROM source AS t

