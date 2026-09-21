{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['resource_name'],
  )
}}

-- Frozen for this dbt run. Downstream models must read this table, not _dlt_loads.

{{ select_latest_dlt_loads('beefy_db', [
  'chains',
  'vault_strategies',
  'address_metadata',
  'bifi_buyback',
  'feebatch_harvests',
  'vault_ids',
  'price_oracles',
]) }}
