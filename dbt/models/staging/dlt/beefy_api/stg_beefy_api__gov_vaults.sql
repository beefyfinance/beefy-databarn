{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['id'],
  )
}}

-- Table, not a view: one copy of the latest completed load per dbt run.
-- No FINAL: an in-flight load must not hide rows from the completed load_id.

WITH source AS (
  SELECT *
  FROM {{ source('dlt', 'beefy_api___gov_vaults') }}
  WHERE _dlt_load_id = {{ latest_dlt_load_id('beefy_api', 'gov_vaults') }}
  LIMIT 1 BY id
)

SELECT
  t.assets,
  t.risks,
  t.point_structure_ids,
  t.earned_oracle_ids,
  t.earned_tokens,
  JSONExtract(coalesce(t.earned_token_addresses, '[]'), 'Array(String)') as earned_token_addresses,
  t.earned_token_decimals,
  t.zaps,
  cast(t.id as String) as id,
  ifNull(t.name, 'Unknown') as name,
  t.type,
  toInt32(t.version) as version,
  t.token,
  cast({{ evm_address('t.token_address') }} as String) as token_address,
  toInt32(t.token_decimals) as token_decimals,
  t.token_provider_id,
  cast({{ evm_address('t.earn_contract_address') }} as String) as earn_contract_address,
  t.earned_token,
  t.oracle,
  t.oracle_id,
  t.status,
  t.created_at,
  cast(t.platform_id as String) as platform_id,
  t.strategy_type_id,
  {{ normalize_network_beefy_key('t.network') }} as network,
  toBool(ifNull(t.is_gov_vault, false)) as is_gov_vault,
  {{ normalize_network_beefy_key('t.chain') }} as chain,
  t.total_supply,
  t.last_harvest,
  t.earned_token_decimals__v_json,
  t.retired_at,
  t.retire_reason,
  cast(ifNull({{ evm_address('t.earned_token_address') }}, '') as String) as earned_token_address,
  t.excluded as excluded,
  t.buy_token_url,
  t.updated_at,
  t.earning_points as earning_points
FROM source AS t

