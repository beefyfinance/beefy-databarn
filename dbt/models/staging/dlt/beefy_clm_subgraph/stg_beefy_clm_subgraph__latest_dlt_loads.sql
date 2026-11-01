{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['resource_name'],
  )
}}

-- Frozen for this dbt run. Downstream models must read this table, not _dlt_loads.

{{ select_latest_dlt_loads('beefy_clm_subgraph', [
  'subgraph_status',
  'clms',
  'classics',
  'tokens',
  'clm_snapshots',
  'classic_snapshots',
  'clm_position_interactions',
  'classic_position_interactions',
  'clm_harvest_events',
  'clm_manager_collection_events',
  'clm_deposit_events',
  'clm_withdraw_events',
  'clm_strategy_tvl_events',
  'transactions',
]) }}
