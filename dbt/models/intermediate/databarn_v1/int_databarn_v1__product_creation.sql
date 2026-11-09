{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['chain_id', 'product_address'],
  )
}}

-- On-chain creation of databarn v1 products, the fallback when envio does not know the contract.
-- Columns stay Nullable: with join_use_nulls=0 a left join miss would otherwise read 0 and 1970.

select
  c.chain_id as chain_id,
  {{ evm_address("splitByChar(':', p.product_key)[4]") }} as product_address,
  s.contract_created_at_block as creation_block,
  s.contract_creation_datetime as creation_datetime
from {{ ref('stg_databarn_v1__import_states') }} s
inner join {{ ref('stg_databarn_v1__products') }} p
  on s.product_id = p.product_id
inner join {{ ref('int_chain_keys') }} c
  on {{ normalize_network_beefy_key('p.chain_key') }} = c.beefy_key
where s.import_type = 'product:investment'
  and s.contract_created_at_block is not null
  and s.contract_creation_datetime is not null
order by p.product_id
limit 1 by chain_id, product_address
