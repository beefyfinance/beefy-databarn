{{
  config(
    materialized='view',
  )
}}

WITH loads_latest AS (
  -- Keep only the latest load per (chain, earn_contract_address) by inserted_at to avoid
  -- duplicates when API entity ids are corrected and reloaded.
  SELECT
    {{ normalize_network_beefy_key('t.chain') }} AS chain,
    cast({{ evm_address('t.earn_contract_address') }} as String) AS earn_contract_address,
    argMax(l.load_id, l.inserted_at) AS load_id
  FROM {{ source('dlt', 'beefy_api___gov_vaults') }} t
  INNER JOIN {{ ref('stg_beefy_api__dlt_loads') }} l ON t._dlt_load_id = l.load_id
  GROUP BY {{ normalize_network_beefy_key('t.chain') }}, cast({{ evm_address('t.earn_contract_address') }} as String)
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
  toBool(t.is_gov_vault) as is_gov_vault,
  {{ normalize_network_beefy_key('t.chain') }} as chain,
  t.total_supply,
  t.last_harvest,
  t.earned_token_decimals__v_json,
  t.retired_at,
  t.retire_reason,
  cast({{ evm_address('t.earned_token_address') }} as String) as earned_token_address,
  t.excluded as excluded,
  t.buy_token_url,
  t.updated_at,
  t.earning_points as earning_points
FROM {{ source('dlt', 'beefy_api___gov_vaults') }} t
INNER JOIN loads_latest ll
  ON ll.chain = {{ normalize_network_beefy_key('t.chain') }}
  AND ll.earn_contract_address = cast({{ evm_address('t.earn_contract_address') }} as String)
  AND t._dlt_load_id = ll.load_id

