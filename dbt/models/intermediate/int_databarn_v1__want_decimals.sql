{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['product_id'],
  )
}}

-- Want token decimals of databarn v1 vaults. The CLM subgraph reads them on-chain, so it wins over the
-- product metadata. Inputs stay Nullable: with join_use_nulls=0 a left join miss would otherwise read 0.

with vaults as (
  select
    p.product_id as product_id,
    c.chain_id as chain_id,
    {{ normalize_network_beefy_key('p.chain_key') }} as chain_key,
    {{ evm_address("splitByChar(':', p.product_key)[4]") }} as product_address,
    {{ evm_address("JSONExtractString(p.product_data, 'vault', 'want_address')") }} as want_address,
    JSONExtract(p.product_data, 'vault', 'want_decimals', 'Nullable(Int64)') as databarn_want_decimals
  from {{ ref('stg_databarn_v1__products') }} p
  inner join {{ ref('int_chain_keys') }} c
    on {{ normalize_network_beefy_key('p.chain_key') }} = c.beefy_key
  where startsWith(p.product_key, 'beefy:vault:')
)

select
  v.product_id as product_id,
  v.chain_id as chain_id,
  v.product_address as product_address,
  v.want_address as want_address,
  v.databarn_want_decimals as databarn_want_decimals,
  t.decimals as subgraph_want_decimals,
  toInt64(coalesce(t.decimals, v.databarn_want_decimals, 18)) as want_decimals
from vaults v
left join (
  select chain_key, {{ evm_address('token_address') }} as token_address, cast(decimals as Nullable(Int64)) as decimals
  from {{ ref('stg_beefy_clm_subgraph__tokens') }}
) t
  on v.chain_key = t.chain_key
  and v.want_address = t.token_address
