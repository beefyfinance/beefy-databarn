{{
  config(
    materialized='table',
    tags=['dimension', 'products'],
    engine='MergeTree()',
    order_by=['chain_id', 'reward_pool_address'],
  )
}}

-- Dimension table: Product (Vault) reference data
-- This table provides product/vault metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  chain_dim.chain_id as chain_id,
  vaults.earn_contract_address as reward_pool_address,
  vaults.id as beefy_key,
  coalesce(vaults.oracle_id, vaults.id) as beefy_price_oracle_key,
  vaults.name as display_name,
  vaults.platform_id as platform_id,
  toBool(ifNull(vaults.status = 'active', false)) as is_active,
  vaults.token_address as underlying_product_address,
  arrayMap(x -> {{ to_representation_evm_address('x') }}, vaults.earned_token_addresses) as reward_token_representation_addresses,
  envio.initialized_block as creation_block,
  envio.initialized_timestamp as creation_datetime,
  toDateTime64(vaults.created_at, 3, 'UTC') as declared_creation_date
FROM {{ ref('stg_beefy_api__gov_vaults') }} vaults
LEFT JOIN {{ ref('chain') }} chain_dim
  ON vaults.network = chain_dim.beefy_key
LEFT JOIN {{ ref('stg_envio__reward_pool') }} envio
  ON chain_dim.chain_id = envio.network_id
  AND vaults.earn_contract_address = envio.address
WHERE vaults.version = 2
