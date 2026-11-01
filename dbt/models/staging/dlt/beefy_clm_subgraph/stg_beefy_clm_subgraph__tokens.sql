{{
  config(
    materialized='view',
  )
}}

-- Amounts and prices are raw on-chain integers: apply token decimals downstream.
SELECT
  t.chain_key AS chain_key,
  {{ evm_address('t.id') }} AS token_address,
  t.symbol AS symbol,
  t.name AS name,
  cast(t.decimals as Int64) AS decimals
FROM {{ source('dlt', 'beefy_clm_subgraph___tokens') }} t FINAL
