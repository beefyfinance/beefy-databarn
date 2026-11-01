-- Fails when a CLM manager with a beefy_db oracle does not resolve to that oracle's asset. The indexer keeps
-- the smallest asset_id when a token has several, so the check applies the same rule. Also fails when the
-- token is missing or has more than one asset.

with oracle_assets as (
  select asset_id, assumeNotNull(beefy_price_oracle_key) as beefy_price_oracle_key
  from {{ ref('asset') }}
  where asset_source = 'beefy_db_oracle'
),

manager_tokens as (
  select
    chain_id,
    representation_address,
    min(asset_id) as consumer_asset_id,
    uniqExact(asset_id) as asset_count
  from {{ ref('token') }}
  where token_kind = 'beefy_clm_manager'
  group by chain_id, representation_address
)

select
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  pk.beefy_share_price_oracle_key as beefy_price_oracle_key,
  o.asset_id as expected_asset_id,
  t.consumer_asset_id as consumer_asset_id,
  t.asset_count as asset_count
from {{ ref('int_product_keys') }} pk
inner join {{ ref('product') }} p
  on pk.chain_id = p.chain_id
  and pk.product_address = p.product_address
  and p.product_type = 'clm_manager'
inner join oracle_assets o
  on pk.beefy_share_price_oracle_key = o.beefy_price_oracle_key
left join manager_tokens t
  on pk.chain_id = t.chain_id
  and pk.product_address = t.representation_address
where ifNull(t.consumer_asset_id, 0) != o.asset_id
  or ifNull(t.asset_count, 0) != 1
