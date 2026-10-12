{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['resource_name'],
    query_settings={
      'max_threads': 2,
      'join_algorithm': 'hash',
    },
  )
}}

-- Frozen for this dbt run. Downstream models must read this table, not _dlt_loads.

{{ select_latest_dlt_loads(
  'beefy_api',
  [
    'vaults',
    'tokens',
    'clm_vaults',
    'cow_vaults',
    'gov_vaults',
    'boosts',
  ],
  exists_only_resources=[
    'apy',
    'treasury',
    'tvl',
    'prices',
    'lps',
    'lps_breakdown',
    'apy_breakdown',
    'mootokenprices',
  ],
) }}
