{{
  config(
    materialized='table',
    tags=['products'],
    engine='MergeTree()',
    order_by=['chain_id', 'boost_address'],
  )
}}


select 
  chain_dim.chain_id as chain_id,
  boost.boost_contract_address as boost_address,
  boost.id as beefy_key,
  coalesce(classic_vault.beefy_wants_price_oracle_key, boost.id) as beefy_wants_price_oracle_key,
  boost.id as display_name,
  toBool(ifNull(boost.status = 'active', false)) as is_active,
  {{ to_representation_evm_address('boost.underlying_token_address') }} as underlying_token_representation_address,
  {{ to_representation_evm_address('boost.reward_token_address') }} as reward_token_representation_address,
  coalesce(envio.initialized_block, creation.creation_block) as creation_block,
  coalesce(envio.initialized_timestamp, creation.creation_datetime) as creation_datetime,
  cast(null as Nullable(DateTime64(3, 'UTC'))) as declared_creation_date
from {{ ref('stg_beefy_api__boosts') }} boost
join {{ ref('dim_chain') }} chain_dim on boost.chain = chain_dim.beefy_key
left join {{ ref('stg_envio__classic_boost') }} envio
  on chain_dim.chain_id = envio.network_id
  and boost.boost_contract_address = envio.address
left join {{ ref('int_databarn_v1__product_creation') }} creation
  on chain_dim.chain_id = creation.chain_id
  and boost.boost_contract_address = creation.product_address
left join {{ ref('dim_product_classic') }} classic_vault
  on chain_dim.chain_id = classic_vault.chain_id
  and boost.pool_id = classic_vault.beefy_key
where boost.version is null or boost.version < 2