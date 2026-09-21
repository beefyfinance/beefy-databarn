{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['resource_name'],
  )
}}

-- Frozen for this dbt run. Downstream models must read this table, not _dlt_loads.

{{ select_latest_dlt_loads('github_files', [
  'beefy_platforms',
  'beefy_ui_chains',
]) }}
