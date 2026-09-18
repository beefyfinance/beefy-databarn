{{
  config(
    materialized='view',
    tags=['marts', 'investor', 'timeline'],
    order_by=['account_id', 'datetime', 'product_address'],
    on_schema_change='sync_all_columns',
  )
}}

-- Mart model: Unified investor timeline tracking user actions (deposits, withdrawals, stakes) by product
-- Materialized view that unifies historical data (from int_investor_timeline_historical) 
-- and recent data (from int_investor_timeline_recent)
-- Uses cutoff date (now - 11 days) to split data: older from historical, newer from recent
-- Provides single query interface for all data with real-time updates
-- Optimized for querying by user address (account_id) and date


{% set cutoff_date_sql %}
    select toDateTime(now() - INTERVAL 11 DAY) as cutoff_date
{% endset %}
{% set cutoff_date_tbl = run_query(cutoff_date_sql) %}
{% if cutoff_date_tbl and cutoff_date_tbl.columns and cutoff_date_tbl.columns[0] and cutoff_date_tbl.columns[0][0] is not none %}
{% set cutoff_date = cutoff_date_tbl.columns[0][0] %}
{% endif %}

with timeline as (
  SELECT 
    datetime,
    account_id,
    product_key,
    chain_id,
    product_address,
    block_number,
    transaction_hash,
    log_index,
    share_to_underlying_price,
    underlying_to_usd_price,
    share_to_usd_price,
    share_balance_after,
    share_balance_before,
    share_balance_diff,
    underlying_balance_after,
    underlying_balance_before,
    underlying_balance_diff,
    usd_balance_before,
    usd_balance_after,
    usd_balance_diff
  FROM {{ ref('int_investor_timeline_historical') }} final
  WHERE datetime < toDateTime('{{ cutoff_date }}')

  UNION ALL

  SELECT
    datetime,
    account_id,
    product_key,
    chain_id,
    product_address,
    block_number,
    transaction_hash,
    log_index,
    share_to_underlying_price,
    underlying_to_usd_price,
    share_to_usd_price,
    share_balance_after,
    share_balance_before,
    share_balance_diff,
    underlying_balance_after,
    underlying_balance_before,
    underlying_balance_diff,
    usd_balance_before,
    usd_balance_after,
    usd_balance_diff
  FROM {{ ref('int_investor_timeline_recent') }}
  WHERE datetime >= toDateTime('{{ cutoff_date }}')
)

SELECT
  t.datetime as datetime,
  t.account_id as account_id,
  t.product_key as product_key,
  p.display_name as product_display_name,
  t.chain_id as chain_id,
  c.chain_name as chain_name,
  p.product_type as product_type,
  t.product_address as product_address,
  NOT p.is_active as is_eol,
  NOT p.is_active as is_dashboard_eol,
  t.block_number as block_number,
  t.transaction_hash as transaction_hash,
  t.log_index as log_index,
  t.share_to_underlying_price as share_to_underlying_price,
  t.underlying_to_usd_price as underlying_to_usd_price,
  t.share_to_usd_price as share_to_usd_price,
  t.share_balance_before as share_balance_before,
  t.share_balance_after as share_balance_after,
  t.share_balance_diff as share_balance_diff,
  t.underlying_balance_before as underlying_balance_before,
  t.underlying_balance_after as underlying_balance_after,
  t.underlying_balance_diff as underlying_balance_diff,
  t.usd_balance_before as usd_balance_before,
  t.usd_balance_after as usd_balance_after,
  t.usd_balance_diff as usd_balance_diff
FROM timeline t
INNER JOIN {{ ref('product') }} p
  ON t.chain_id = p.chain_id
  AND t.product_address = p.product_address
INNER JOIN {{ ref('chain') }} c
  ON t.chain_id = c.chain_id
