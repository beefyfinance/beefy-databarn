{{
  config(
    materialized='table',
    tags=['dimension', 'accounts'],
    engine='MergeTree()',
    order_by=['address'],
  )
}}

-- Dimension table: Account reference data
-- This table provides a unified reference for all addresses in the system
-- with labels identifying their roles (products, strategies, tokens, smart contracts)

WITH product_addresses AS (
  SELECT
    chain_id,
    product_address as address,
    product_type as label,
    true as is_contract
  FROM {{ ref('product') }}
  WHERE product_address IS NOT NULL
    AND product_address != ''
),

strategy_addresses AS (
  -- CLM Strategy addresses
  SELECT
    c.chain_id as chain_id,
    address,
    'clm_strategy' as label,
    true as is_contract
  FROM {{ ref('stg_envio__clm_strategy') }}
  LEFT JOIN {{ ref('chain') }} c
    ON network_id = c.network_id
  WHERE address IS NOT NULL
    AND address != ''
  
  UNION ALL
  
  -- Classic Vault Strategy addresses
  SELECT    
    c.chain_id as chain_id,
    address,
    'classic_vault_strategy' as label,
    true as is_contract
  FROM {{ ref('stg_envio__classic_vault_strategy') }}
  LEFT JOIN {{ ref('chain') }} c
    ON network_id = c.network_id
  WHERE address IS NOT NULL
    AND address != ''
),

token_addresses AS (
  -- ERC20 Token addresses (nullable field)
  SELECT
    chain_id,
    erc20_address as address,
    'erc20' as label,
    true as is_contract  -- ERC20 tokens are smart contracts
  FROM {{ ref('token') }}
  WHERE erc20_address IS NOT NULL
    AND erc20_address != ''
),

underlying_token_addresses AS (
  -- Underlying tokens from CLM products (array)
  SELECT
    chain_id,
    addr as address,
    'underlying_token' as label,
    true as is_contract  -- Will be overridden by metadata if available
  FROM {{ ref('product_clm') }}
  ARRAY JOIN underlying_token_representation_addresses as addr
  WHERE addr IS NOT NULL
    AND addr != ''
    AND addr != '0x0000000000000000000000000000000000000000'
  
  UNION ALL
  
  -- LP token from CLM products
  SELECT
    chain_id,
    lp_token_representation_address as address,
    'lp_token' as label,
    true as is_contract
  FROM {{ ref('product_clm') }}
  WHERE lp_token_representation_address IS NOT NULL
    AND lp_token_representation_address != ''
    AND lp_token_representation_address != '0x0000000000000000000000000000000000000000'
  
  UNION ALL
  
  -- Underlying token from Classic products
  SELECT
    chain_id,
    underlying_token_representation_address as address,
    'smart_contract' as label,
    true as is_contract
  FROM {{ ref('product_classic') }}
  WHERE underlying_token_representation_address IS NOT NULL
    AND underlying_token_representation_address != ''
    AND underlying_token_representation_address != '0x0000000000000000000000000000000000000000'
  
  UNION ALL
  
  -- Underlying token from Classic Boost
  SELECT
    chain_id,
    underlying_token_representation_address as address,
    'underlying_token' as label,
    true as is_contract
  FROM {{ ref('product_classic_boost') }}
  WHERE underlying_token_representation_address IS NOT NULL
    AND underlying_token_representation_address != ''
    AND underlying_token_representation_address != '0x0000000000000000000000000000000000000000'
  
  UNION ALL
  
  -- Reward token from Classic Boost
  SELECT
    chain_id,
    reward_token_representation_address as address,
    'reward_token' as label,
    true as is_contract
  FROM {{ ref('product_classic_boost') }}
  WHERE reward_token_representation_address IS NOT NULL
    AND reward_token_representation_address != ''
    AND reward_token_representation_address != '0x0000000000000000000000000000000000000000'
  
  UNION ALL
  
  -- Reward tokens from Reward Pool (array)
  SELECT
    chain_id,
    addr as address,
    'reward_token' as label,
    true as is_contract
  FROM {{ ref('product_reward_pool') }}
  ARRAY JOIN reward_token_representation_addresses as addr
  WHERE addr IS NOT NULL
    AND addr != ''
    AND addr != '0x0000000000000000000000000000000000000000'
),

address_metadata AS (
  SELECT
    c.chain_id as chain_id,
    address,
    is_contract,
    label
  FROM {{ ref('stg_beefy_db__address_metadata') }}
  LEFT JOIN {{ ref('chain') }} c
    ON chain_id = c.network_id
  where is_contract = true or label is not null
),

special_addresses AS (
    -- Zero address on all chains
    SELECT
      c.chain_id as chain_id,
      '0x0000000000000000000000000000000000000000' as address,
      'zero_address' as label,
      false as is_contract
    FROM {{ ref('chain') }} c
    
    UNION ALL
    
    -- Dead address on all chains
    SELECT
      c.chain_id as chain_id,
      '0x000000000000000000000000000000000000dead' as address,
      'dead_address' as label,
      false as is_contract
    FROM {{ ref('chain') }} c
),

address_activity AS (
  SELECT
    a.address,
    min(nullIf(tbc.block_timestamp, toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))) as first_activity_timestamp,
    max(nullIf(tbc.block_timestamp, toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))) as last_activity_timestamp,
    count(*) as total_balance_changes,
    count(DISTINCT tbc.trx_hash) as unique_transactions,
    count(DISTINCT tbc.token_id) as unique_tokens,
    arrayDistinct(groupArray(cast(tbc.network_id as Int64))) as activity_chain_ids
  FROM {{ ref('stg_envio__token_balance_change') }} tbc
  INNER JOIN {{ ref('stg_envio__account') }} a
    ON tbc.account_id = a.id
  GROUP BY a.address
),

activity_addresses AS (
  -- Extract addresses from activity data and expand chain_ids
  SELECT
    act.address,
    chain_id,
    CAST(NULL as Nullable(String)) as label,
    false as is_contract
  FROM address_activity act
  ARRAY JOIN act.activity_chain_ids as chain_id
  WHERE act.address IS NOT NULL
    AND act.address != ''
),

zap_addresses AS (
  SELECT
    network_id as chain_id,
    caller_address as address,
    CAST(NULL as Nullable(String)) as label,
    false as is_contract
  FROM {{ ref('stg_beefy_db__zap_records') }}
  WHERE caller_address IS NOT NULL
    AND caller_address != ''

  UNION ALL

  SELECT
    network_id as chain_id,
    recipient_address as address,
    CAST(NULL as Nullable(String)) as label,
    false as is_contract
  FROM {{ ref('stg_beefy_db__zap_records') }}
  WHERE recipient_address IS NOT NULL
    AND recipient_address != ''

  UNION ALL

  SELECT
    network_id as chain_id,
    from_address as address,
    CAST(NULL as Nullable(String)) as label,
    false as is_contract
  FROM {{ ref('stg_beefy_db__zap_parent_transactions') }}
  WHERE from_address IS NOT NULL
    AND from_address != ''

  UNION ALL

  SELECT
    network_id as chain_id,
    to_address as address,
    'smart_contract' as label,
    true as is_contract
  FROM {{ ref('stg_beefy_db__zap_parent_transactions') }}
  WHERE to_address IS NOT NULL
    AND to_address != ''

  UNION ALL

  SELECT
    network_id as chain_id,
    token_address as address,
    'erc20' as label,
    true as is_contract
  FROM {{ ref('stg_beefy_db__zap_token_transfers_v2') }}
  WHERE token_address IS NOT NULL
    AND token_address != ''
    AND token_address != '0x0000000000000000000000000000000000000000'

  UNION ALL

  SELECT
    network_id as chain_id,
    from_address as address,
    CAST(NULL as Nullable(String)) as label,
    false as is_contract
  FROM {{ ref('stg_beefy_db__zap_failures') }}
  WHERE from_address IS NOT NULL
    AND from_address != ''
),

all_addresses AS (
  SELECT chain_id, address, label, is_contract FROM product_addresses
  UNION ALL
  SELECT chain_id, address, label, is_contract FROM strategy_addresses
  UNION ALL
  SELECT chain_id, address, label, is_contract FROM token_addresses
  UNION ALL
  SELECT chain_id, address, label, is_contract FROM underlying_token_addresses
  UNION ALL
  SELECT chain_id, address, label, is_contract FROM address_metadata
  UNION ALL
  SELECT chain_id, address, label, is_contract FROM special_addresses
  UNION ALL
  SELECT chain_id, address, label, is_contract FROM activity_addresses
  UNION ALL
  SELECT chain_id, address, label, is_contract FROM zap_addresses
),

addresses_aggregated AS (
  SELECT
    address,
    arrayDistinct(arrayFilter(x -> x IS NOT NULL, groupArray(label))) as labels,
    coalesce(max(is_contract), false) as is_contract,
    arrayDistinct(arrayFlatten(groupArray(cast(chain_id as Int64)))) as active_chain_ids
  FROM all_addresses
  WHERE address IS NOT NULL
  GROUP BY address
)

SELECT
  cast(aa.address as String) as address,
  aa.labels,
  aa.is_contract,
  aa.active_chain_ids,
  toNullable(act.first_activity_timestamp) as first_activity_timestamp,
  toNullable(act.last_activity_timestamp) as last_activity_timestamp,
  cast(coalesce(act.total_balance_changes, 0) as UInt64) as total_balance_changes,
  cast(coalesce(act.unique_transactions, 0) as UInt64) as unique_transactions,
  cast(coalesce(act.unique_tokens, 0) as UInt64) as unique_tokens
FROM addresses_aggregated aa
LEFT JOIN address_activity act
  ON aa.address = act.address

