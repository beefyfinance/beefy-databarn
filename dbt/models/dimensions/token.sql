{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['dimension', 'tokens'],
    order_by=['chain_id', 'representation_address'],
  )
}}

-- Dimension table: Token reference data
-- This table provides token metadata for joining with fact tables
-- Small reference table, materialized as table for performance

with tokens as (
  SELECT
    chain_dim.chain_id as chain_id,

    {{ to_representation_evm_address('tokens.address') }} as representation_address,

    case 
      when tokens.type = 'erc20' then cast(tokens.address as String) 
      else null 
    end as erc20_address,
    
    ifNull(toBool(tokens.type = 'native'), false) as is_native,

    tokens.symbol,
    tokens.name,
    tokens.decimals
    -- oracle,
    -- oracle_id,
    -- type,
    -- bridge,
    -- staked
  FROM {{ ref('stg_beefy_api__tokens') }} tokens
  JOIN {{ ref('chain') }} chain_dim
    ON tokens.chain_beefy_key = chain_dim.beefy_key
)
-- some tokens get duplicated in the source data, so we need to deduplicate
select distinct on (chain_id, representation_address) * 
from tokens