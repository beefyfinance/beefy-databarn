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
  {{ normalize_network_beefy_key('ui.chain_key') }} as beefy_key,
  -- Price oracle of the chain's native gas token; prices harvest native_price (fees).
  cast(nullIf(ui.native_token_oracle_id, '') as Nullable(String)) as beefy_native_price_oracle_key
from {{ ref('stg_github_files___beefy_ui_chains') }} ui
