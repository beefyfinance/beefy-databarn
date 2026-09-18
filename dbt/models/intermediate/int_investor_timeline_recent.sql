{{
  config(
    materialized='view',
    tags=['intermediate', 'investor', 'timeline'],
  )
}}

-- Intermediate model: Recent investor timeline data (last 24 hours)
-- View for always-fresh recent data with low memory footprint
-- Limited to last 24 hours to avoid memory issues
-- Filters all tables (balance changes and prices) by datetime for efficiency

WITH filtered_balance_changes AS (
  SELECT
    *
  FROM {{ ref('stg_envio__token_balance_change') }}
  WHERE
    -- Only last 24 hours
    block_timestamp >= now() - INTERVAL 15 DAY
),

filtered_prices AS (
  SELECT
    chain_id,
    token_address,
    date_time,
    price
  FROM {{ ref('int_product_price') }}
  WHERE
    -- Filter price table by datetime range for efficiency
    -- Match the date range of balance changes: last 24 hours
    date_time >= now() - INTERVAL 15 DAY
)

SELECT
  tbc.block_timestamp as datetime,
  tbc.account_id as account_id,
  p.beefy_key as product_key,
  tbc.chain_id as chain_id,
  tbc.token_contract_address as product_address,

  tbc.block_number as block_number,
  tbc.trx_hash as transaction_hash,
  tbc.log_index as log_index,

  cast(NULL as Nullable(Decimal(76, 20))) as share_to_underlying_price,
  cast(NULL as Nullable(Decimal(76, 20))) as underlying_to_usd_price,
  share_price.price as share_to_usd_price,

  -- Balances
  tbc.balance_after as share_balance_after,
  tbc.balance_before as share_balance_before,
  tbc.share_diff as share_balance_diff,

  cast(NULL as Nullable(Decimal(76, 20))) as underlying_balance_after,
  cast(NULL as Nullable(Decimal(76, 20))) as underlying_balance_before,
  cast(NULL as Nullable(Decimal(76, 20))) as underlying_balance_diff,

  {{ to_decimal('tbc.balance_before * coalesce(share_price.price, 0)') }} as usd_balance_before,
  {{ to_decimal('tbc.balance_after * coalesce(share_price.price, 0)') }} as usd_balance_after,
  {{ to_decimal('(tbc.balance_after - tbc.balance_before) * coalesce(share_price.price, 0)') }} as usd_balance_diff
FROM filtered_balance_changes tbc
INNER JOIN {{ ref('int_product_keys') }} p
  ON tbc.chain_id = p.chain_id
  AND tbc.token_contract_address = p.product_address
ASOF LEFT JOIN filtered_prices share_price
  ON tbc.chain_id = share_price.chain_id
  AND tbc.token_contract_address = share_price.token_address
  AND tbc.block_timestamp >= share_price.date_time

