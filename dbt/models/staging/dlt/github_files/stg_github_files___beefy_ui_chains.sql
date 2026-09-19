{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  ifNull(t.name, 'Unknown') as name,
  cast(t.chain_key as String) as chain_key,
  {{ to_str_list('t.rpc') }} as rpc,
  t.explorer_url,
  {{ evm_address('t.multicall3_address') }} as multicall3_address,
  {{ evm_address('t.app_multicall_contract_address') }} as app_multicall_contract_address,
  JSONExtractString(t.native, 'symbol') as native_token_symbol,
  JSONExtractString(t.native, 'oracleId') as native_token_oracle_id,
  toInt64(JSONExtractString(t.native, 'decimals')) as native_token_decimals,
  JSONExtractString(t.gas, 'type') as gas_type,
  case when t.eol is null then null else toDateTime(toInt64(t.eol)) end as eol_date_time,
  toBool(t.disabled) as ui_disabled,
  toBool(t.new) as ui_new
FROM {{ source('dlt', 'github_files___beefy_ui_chains') }} t FINAL

