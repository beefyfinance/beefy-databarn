{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['chain_id', 'product_address'],
  )
}}

-- Classic vaults known to databarn v1 but gone from the Beefy API. Their beefy_db harvests and TVL
-- are dropped by joins on int_product_keys until they exist as products.
-- A vault id still in use by the API (redeployed vault) is left out: beefy_db facts join on the id,
-- so a second product with that id would fan them out. Gov vaults are left out too.
-- Other ids are global: tvl facts join on beefy_key alone.

with api_keys as (
  select id as beefy_key from {{ ref('stg_beefy_api__vaults') }}
  union distinct
  select id as beefy_key from {{ ref('stg_beefy_api__clm_vaults') }}
  union distinct
  select id as beefy_key from {{ ref('stg_beefy_api__gov_vaults') }}
  union distinct
  select id as beefy_key from {{ ref('stg_beefy_api__boosts') }}
),

vaults as (
  select
    p.product_id as databarn_v1_product_id,
    c.chain_id as chain_id,
    {{ evm_address("splitByChar(':', p.product_key)[4]") }} as product_address,
    JSONExtractString(p.product_data, 'vault', 'id') as beefy_key,
    nullIf(JSONExtractString(p.product_data, 'vault', 'want_price_feed_key'), '') as want_price_feed_key,
    nullIf(JSONExtractString(p.product_data, 'vault', 'want_address'), '') as want_address,
    JSONLength(p.product_data, 'vault', 'assets') > 1 as want_is_lp,
    nullIf(JSONExtractString(p.product_data, 'vault', 'token_name'), '') as share_token_symbol,
    nullIf(JSONExtractString(p.product_data, 'vault', 'platform_id'), '') as platform_id
  from {{ ref('stg_databarn_v1__products') }} p
  inner join {{ ref('int_chain_keys') }} c
    on {{ normalize_network_beefy_key('p.chain_key') }} = c.beefy_key
  where startsWith(p.product_key, 'beefy:vault:')
    and not JSONExtractBool(p.product_data, 'vault', 'is_gov_vault')
)

select
  v.chain_id as chain_id,
  v.product_address as product_address,
  v.beefy_key as beefy_key,
  cast(coalesce(v.want_price_feed_key, v.beefy_key) as String) as beefy_wants_price_oracle_key,
  -- Some retired platforms are gone from the platform dimension.
  if(v.platform_id in (select id from {{ ref('stg_github_files___beefy_platforms') }}), v.platform_id, null) as platform_id,
  {{ to_representation_evm_address('v.want_address') }} as underlying_token_representation_address,
  v.want_address as want_address,
  d.want_decimals as want_decimals,
  v.want_is_lp as want_is_lp,
  coalesce(v.share_token_symbol, v.beefy_key) as share_token_symbol
from vaults v
inner join {{ ref('int_databarn_v1__want_decimals') }} d
  on v.databarn_v1_product_id = d.product_id
where v.beefy_key != ''
  and v.beefy_key not in (select beefy_key from api_keys)
order by v.databarn_v1_product_id
limit 1 by v.beefy_key
