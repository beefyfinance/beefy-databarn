{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['chain_id', 'product_address'],
  )
}}

-- Skinny product identity map. Grain is (chain_id, product_address).
-- beefy_key is 1:1; beefy_wants_price_oracle_key is 1:many. It prices the beefy wants (deposit) token, NOT the vault share.
-- Share prices come from share_token_symbol (/mootokenprices); see int_product_share_price.
-- Join keys for API snapshots live here; display attributes stay on the product dimension.

with classic_products as (
  select
    c.chain_id as chain_id,
    vaults.earn_contract_address as product_address,
    vaults.id as beefy_key,
    coalesce(vaults.oracle_id, vaults.id) as beefy_wants_price_oracle_key,
    cast(nullIf(vaults.earned_token, '') as Nullable(String)) as share_token_symbol,
    cast(nullIf(vaults.oracle, '') as Nullable(String)) as beefy_wants_price_oracle_type
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
    coalesce(vaults.oracle_id, vaults.id) as beefy_wants_price_oracle_key,
    cast(nullIf(vaults.earned_token, '') as Nullable(String)) as share_token_symbol,
    cast(nullIf(vaults.oracle, '') as Nullable(String)) as beefy_wants_price_oracle_type
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
    coalesce(vaults.oracle_id, vaults.id) as beefy_wants_price_oracle_key,
    cast(nullIf(vaults.earned_token, '') as Nullable(String)) as share_token_symbol,
    cast(nullIf(vaults.oracle, '') as Nullable(String)) as beefy_wants_price_oracle_type
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
    coalesce(classic_products.beefy_wants_price_oracle_key, boosts.id) as beefy_wants_price_oracle_key,
    -- Boost earned token is the reward token, not the vault share token.
    cast(NULL as Nullable(String)) as share_token_symbol,
    classic_products.beefy_wants_price_oracle_type as beefy_wants_price_oracle_type
  from {{ ref('stg_beefy_api__boosts') }} boosts
  inner join {{ ref('int_chain_keys') }} c
    on boosts.chain = c.beefy_key
  left join classic_products
    on c.chain_id = classic_products.chain_id
    and {{ to_representation_evm_address('boosts.underlying_token_address') }} = classic_products.product_address
  where boosts.version is null or boosts.version < 2
),

products as (
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_wants_price_oracle_type from classic_products
  union all
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_wants_price_oracle_type from clm_products
  union all
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_wants_price_oracle_type from reward_pool_products
  union all
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_wants_price_oracle_type from boost_products
)

select
  p.chain_id as chain_id,
  p.product_address as product_address,
  p.beefy_key as beefy_key,
  p.beefy_wants_price_oracle_key as beefy_wants_price_oracle_key,
  p.beefy_wants_price_oracle_type as beefy_wants_price_oracle_type,
  o.id as beefy_wants_price_oracle_id,
  p.share_token_symbol as share_token_symbol
from products p
left join {{ ref('stg_beefy_db__price_oracles') }} o
  on p.beefy_wants_price_oracle_key = o.oracle_id
