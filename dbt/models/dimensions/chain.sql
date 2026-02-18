{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['dimension', 'chains']
  )
}}

-- Dimension table: Chain reference data
-- This table provides chain metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  ui.network_id as chain_id,
  ui.network_id,
  ifNull(ui.name, 'Unknown') as chain_name,
  {{ normalize_network_beefy_key('ui.chain_key') }} as beefy_key,
  ifNull(toBool(case 
    when ui.eol_date_time is null then true
    when db.enabled is not null then db.enabled
    when ui.ui_disabled is not null then not ui.ui_disabled
    else false
  end), false) as beefy_enabled,
  ui.rpc as public_rpc_urls,
  ui.explorer_url,
  cast(ui.multicall3_address as String) as multicall3_address,
  cast(ui.app_multicall_contract_address as String) as ui_lens_address,
  ui.gas_type,
  ui.eol_date_time
FROM {{ ref('stg_github_files___beefy_ui_chains') }} ui
 LEFT JOIN {{ ref('stg_beefy_db__chains') }} db
  ON ui.network_id = db.network_id