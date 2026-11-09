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
-- Share prices come from share_token_symbol (/mootokenprices); see int_price_series_map.
-- CLM managers and reward pools are the exception: beefy_share_price_oracle_key is the beefy_db oracle that prices
-- the share. A CLM oracle prices the manager share, and a reward pool share is worth its underlying token.
-- A boost receipt is worth the vault share it stakes: boost_staked_product_address, from the boost's pool_id
-- (the vault id). The API omits the boost's token address on about half of the boosts.
-- Join keys for API snapshots live here; display attributes stay on the product dimension.

with classic_products as (
  select
    c.chain_id as chain_id,
    vaults.earn_contract_address as product_address,
    vaults.id as beefy_key,
    coalesce(vaults.oracle_id, vaults.id) as beefy_wants_price_oracle_key,
    cast(nullIf(vaults.earned_token, '') as Nullable(String)) as share_token_symbol,
    cast(null as Nullable(String)) as beefy_share_price_oracle_key,
    cast(nullIf(vaults.oracle, '') as Nullable(String)) as beefy_wants_price_oracle_type,
    cast(null as Nullable(String)) as boost_staked_product_address
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
    cast(nullIf(vaults.oracle_id, '') as Nullable(String)) as beefy_share_price_oracle_key,
    cast(nullIf(vaults.oracle, '') as Nullable(String)) as beefy_wants_price_oracle_type,
    cast(null as Nullable(String)) as boost_staked_product_address
  from {{ ref('stg_beefy_api__clm_vaults') }} vaults
  inner join {{ ref('int_chain_keys') }} c
    on vaults.network = c.beefy_key
  where ifNull(vaults.is_gov_vault, false) = false
),

-- Gone from the API, known to databarn v1. Priced like a classic vault: /mootokenprices keeps serving most of them.
-- No symbol, so databarn ppfs only, when a live vault uses it (/mootokenprices joins on it) or when the want is
-- not 18 decimals (stg_beefy_api__mootokenprices can only rescale vaults in the API).
retired_products as (
  select
    r.chain_id as chain_id,
    r.product_address as product_address,
    r.beefy_key as beefy_key,
    r.beefy_wants_price_oracle_key as beefy_wants_price_oracle_key,
    if(
      r.want_decimals = 18
      and (r.chain_id, r.share_token_symbol) not in (select chain_id, share_token_symbol from classic_products),
      cast(r.share_token_symbol as Nullable(String)),
      null
    ) as share_token_symbol,
    cast(null as Nullable(String)) as beefy_share_price_oracle_key,
    cast(null as Nullable(String)) as beefy_wants_price_oracle_type,
    cast(null as Nullable(String)) as boost_staked_product_address
  from {{ ref('int_databarn_v1__retired_vaults') }} r
),

reward_pool_products as (
  select
    c.chain_id as chain_id,
    vaults.earn_contract_address as product_address,
    vaults.id as beefy_key,
    coalesce(vaults.oracle_id, vaults.id) as beefy_wants_price_oracle_key,
    cast(nullIf(vaults.earned_token, '') as Nullable(String)) as share_token_symbol,
    cast(nullIf(vaults.oracle_id, '') as Nullable(String)) as beefy_share_price_oracle_key,
    cast(nullIf(vaults.oracle, '') as Nullable(String)) as beefy_wants_price_oracle_type,
    cast(null as Nullable(String)) as boost_staked_product_address
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
    cast(null as Nullable(String)) as beefy_share_price_oracle_key,
    classic_products.beefy_wants_price_oracle_type as beefy_wants_price_oracle_type,
    cast(nullIf(classic_products.product_address, '') as Nullable(String)) as boost_staked_product_address
  from {{ ref('stg_beefy_api__boosts') }} boosts
  inner join {{ ref('int_chain_keys') }} c
    on boosts.chain = c.beefy_key
  left join classic_products
    on c.chain_id = classic_products.chain_id
    and boosts.pool_id = classic_products.beefy_key
  where boosts.version is null or boosts.version < 2
),

products as (
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_share_price_oracle_key, beefy_wants_price_oracle_type, boost_staked_product_address from classic_products
  union all
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_share_price_oracle_key, beefy_wants_price_oracle_type, boost_staked_product_address from clm_products
  union all
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_share_price_oracle_key, beefy_wants_price_oracle_type, boost_staked_product_address from retired_products
  union all
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_share_price_oracle_key, beefy_wants_price_oracle_type, boost_staked_product_address from reward_pool_products
  union all
  select chain_id, product_address, beefy_key, beefy_wants_price_oracle_key, share_token_symbol, beefy_share_price_oracle_key, beefy_wants_price_oracle_type, boost_staked_product_address from boost_products
)

select
  p.chain_id as chain_id,
  p.product_address as product_address,
  p.beefy_key as beefy_key,
  p.beefy_wants_price_oracle_key as beefy_wants_price_oracle_key,
  p.beefy_wants_price_oracle_type as beefy_wants_price_oracle_type,
  o.id as beefy_wants_price_oracle_id,
  p.share_token_symbol as share_token_symbol,
  p.beefy_share_price_oracle_key as beefy_share_price_oracle_key,
  p.boost_staked_product_address as boost_staked_product_address
from products p
left join {{ ref('stg_beefy_db__price_oracles') }} o
  on p.beefy_wants_price_oracle_key = o.oracle_id
