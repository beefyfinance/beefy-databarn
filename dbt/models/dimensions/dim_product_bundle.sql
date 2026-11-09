{{
    config(
        materialized='table',
        tags=['products'],
        engine='MergeTree()',
        order_by=['chain_id', 'bundle_root_product_address'],
    )
}}


with clm_bundles as (
    select 
        clm.chain_id as chain_id,
        {{ if_null('classic.vault_address', 'clm.vault_address') }} as bundle_root_product_address,
        'clm_bundle' as bundle_type,
        clm.is_active as is_active,
        clm.beefy_key as beefy_key,
        clm.display_name as display_name,
        clm.platform_id as platform_id,
        groupArray(10)(DISTINCT reward_pool.reward_pool_address) as reward_pool_addresses,
        groupArray(10)(DISTINCT boost.boost_address) as classic_boost_addresses,
        clm.creation_block as creation_block,
        clm.creation_datetime as creation_datetime
    from {{ ref('dim_product_clm') }} clm
    left join {{ ref('dim_product_classic') }} classic
        on classic.chain_id = clm.chain_id
        and classic.underlying_token_representation_address = clm.vault_address
    left join {{ ref('dim_product_reward_pool') }} reward_pool 
        on reward_pool.chain_id = clm.chain_id 
        and (
            reward_pool.underlying_product_address = clm.vault_address 
            or reward_pool.underlying_product_address = classic.vault_address
        )
    left join {{ ref('dim_product_classic_boost') }} boost
        on boost.chain_id = clm.chain_id
        and boost.underlying_token_representation_address = clm.vault_address
    group by 1,2,3,4,5,6,7,10,11
),
classic_bundles as (
    select 
        classic.chain_id,
        classic.vault_address as bundle_root_product_address,
        'classic_bundle' as bundle_type,
        classic.is_active as is_active,
        classic.beefy_key as beefy_key,
        classic.display_name as display_name,
        classic.platform_id as platform_id,
        groupArray(10)(DISTINCT reward_pool.reward_pool_address) as reward_pool_addresses,
        groupArray(10)(DISTINCT boost.boost_address) as classic_boost_addresses,
        classic.creation_block as creation_block,
        classic.creation_datetime as creation_datetime
    from {{ ref('dim_product_classic') }} classic
    left join {{ ref('dim_product_reward_pool') }} reward_pool 
        on reward_pool.chain_id = classic.chain_id
        and reward_pool.underlying_product_address = classic.vault_address
    left join {{ ref('dim_product_classic_boost') }} boost
        on boost.chain_id = classic.chain_id
        and boost.underlying_token_representation_address = classic.vault_address
    where
        (classic.chain_id, classic.vault_address) not in (
            select chain_id, bundle_root_product_address
            from clm_bundles
        )
    group by 1,2,3,4,5,6,7,10,11
)
select * from clm_bundles
union all
select * from classic_bundles