{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['chain_id'],
  )
}}

-- Skinny chain identity map. int_ models join this instead of the chain dimension.
select
  ui.network_id as chain_id,
  ui.network_id as network_id,
  {{ normalize_network_beefy_key('ui.chain_key') }} as beefy_key
from {{ ref('stg_github_files___beefy_ui_chains') }} ui
