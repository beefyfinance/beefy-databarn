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

SELECT 
  datetime,
  account_id,
  product_key,
  product_display_name,
  chain_id,
  chain_name,
  product_type,
  product_address,
  is_eol,
  is_dashboard_eol,
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
  product_display_name,
  chain_id,
  chain_name,
  product_type,
  product_address,
  is_eol,
  is_dashboard_eol,
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
