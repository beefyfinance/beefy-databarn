{{
  config(
    materialized='table',
    tags=['dimension', 'products'],
    engine='MergeTree()',
    order_by=['chain_id', 'vault_address'],
  )
}}

-- Dimension table: Product (Vault) reference data
-- This table provides product/vault metadata for joining with fact tables
-- Small reference table, materialized as table for performance

WITH strategies_agg AS (
  SELECT
    classic_vault.id as classic_vault_id,
    classic_vault.network_id,
    classic_vault.address as vault_address,
    if(
      count(strategy.address) > 0,
      '[' || arrayStringConcat(
        groupArray(
          '{' ||
          '"address":' || toJSONString(strategy.address) || ',' ||
          '"initialized_block":' || toJSONString(toInt64OrNull(trim(strategy.initialized_block))) || ',' ||
          '"initialized_timestamp":' || toJSONString(strategy.initialized_timestamp) ||
          '}'
        ),
        ','
      ) || ']',
      '[]'
    ) as strategies_json
  FROM {{ ref('stg_envio__classic_vault') }} classic_vault
  LEFT JOIN {{ ref('stg_envio__classic_vault_strategy') }} strategy
    ON classic_vault.id = strategy.classic_vault_id
    AND strategy.address IS NOT NULL
    AND strategy.address != ''
    AND strategy.initialized_block IS NOT NULL
    AND trim(strategy.initialized_block) != ''
    AND length(trim(strategy.initialized_block)) > 0
    AND toInt64OrNull(trim(strategy.initialized_block)) IS NOT NULL
    AND strategy.initialized_timestamp IS NOT NULL
    AND strategy.initialized_timestamp >= '2020-01-01'
  GROUP BY classic_vault.id, classic_vault.network_id, classic_vault.address
)
SELECT
  chain_dim.chain_id as chain_id,
  vaults.earn_contract_address as vault_address,
  vaults.id as beefy_key,
  coalesce(vaults.oracle_id, vaults.id) as beefy_price_oracle_key,
  vaults.name as display_name,
  vaults.platform_id as platform_id,
  toBool(ifNull(vaults.status = 'active', false)) as is_active,
  vaults.strategy as latest_strategy_address,
  {{ to_representation_evm_address('vaults.token_address') }} as underlying_token_representation_address,
  envio.initialized_block as creation_block,
  envio.initialized_timestamp as creation_datetime,
  toDateTime64(vaults.created_at, 3, 'UTC') as declared_creation_date,
  coalesce(nullIf(strategies_agg.strategies_json, ''), '[]') as strategies_json
FROM {{ ref('stg_beefy_api__vaults') }} vaults
LEFT JOIN {{ ref('chain') }} chain_dim
  ON vaults.network = chain_dim.beefy_key
LEFT JOIN {{ ref('stg_envio__classic_vault') }} envio
  ON chain_dim.chain_id = envio.network_id
  AND vaults.earn_contract_address = envio.address
LEFT JOIN strategies_agg
  ON chain_dim.chain_id = strategies_agg.network_id
  AND vaults.earn_contract_address = strategies_agg.vault_address
WHERE NOT vaults.is_gov_vault
