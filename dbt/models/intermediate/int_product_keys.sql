{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['chain_id', 'product_address'],
  )
}}

-- Skinny product identity map. Grain is (chain_id, product_address).
-- beefy_key is 1:1; oracle_key is 1:many (prices fan out to products).
-- Display attributes stay on the product dimension.

with classic_products as (
  select
    c.chain_id as chain_id,
    vaults.earn_contract_address as product_address,
    vaults.id as beefy_key,
    coalesce(vaults.oracle_id, vaults.id) as oracle_key
  from {{ ref('stg_beefy_api__vaults') }} vaults
  inner join {{ ref('int_chain_keys') }} c
    on vaults.network = c.beefy_key
  where ifNull(vaults.is_gov_vault, false) = false
),

clm_products as (
  select
    c.chain_id as chain_id,
    vaults.earn_contract_address as product_address,
    vaults.id as beefy_key,
    coalesce(vaults.oracle_id, vaults.id) as oracle_key
  from {{ ref('stg_beefy_api__clm_vaults') }} vaults
  inner join {{ ref('int_chain_keys') }} c
    on vaults.network = c.beefy_key
  where ifNull(vaults.is_gov_vault, false) = false
),

reward_pool_products as (
  select
    c.chain_id as chain_id,
    vaults.earn_contract_address as product_address,
    vaults.id as beefy_key,
    coalesce(vaults.oracle_id, vaults.id) as oracle_key
  from {{ ref('stg_beefy_api__gov_vaults') }} vaults
  inner join {{ ref('int_chain_keys') }} c
    on vaults.network = c.beefy_key
  where vaults.version = 2
),

boost_products as (
  select
    c.chain_id as chain_id,
    boosts.boost_contract_address as product_address,
    boosts.id as beefy_key,
    coalesce(classic_products.oracle_key, boosts.id) as oracle_key
  from {{ ref('stg_beefy_api__boosts') }} boosts
  inner join {{ ref('int_chain_keys') }} c
    on boosts.chain = c.beefy_key
  left join classic_products
    on c.chain_id = classic_products.chain_id
    and {{ to_representation_evm_address('boosts.underlying_token_address') }} = classic_products.product_address
  where boosts.version is null or boosts.version < 2
)

select chain_id, product_address, beefy_key, oracle_key
from (
  select chain_id, product_address, beefy_key, oracle_key from classic_products
  union all
  select chain_id, product_address, beefy_key, oracle_key from clm_products
  union all
  select chain_id, product_address, beefy_key, oracle_key from reward_pool_products
  union all
  select chain_id, product_address, beefy_key, oracle_key from boost_products
)
