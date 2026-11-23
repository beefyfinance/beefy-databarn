{{
  config(
    materialized='view',
  )
}}

-- tvl_by_chain.chain_id is the internal id of chain_ids, not the EVM network id: chain_key and
-- network_id (NULL for a chain missing from `chains`) are looked up from it.
SELECT
  cast(t.chain_id as Int64) as chain_id,
  ci.chain_key as chain_key,
  nullIf(c.network_id, 0) as network_id,
  cast(t.t as DateTime('UTC')) as date_time,
  {{ to_decimal('t.total') }} as tvl_usd,
  {{ to_decimal('t.vault') }} as vault_tvl_usd,
  {{ to_decimal('t.gov') }} as gov_tvl_usd,
  {{ to_decimal('t.clm') }} as clm_tvl_usd
FROM {{ source('dlt', 'beefy_db___tvl_by_chain') }} t FINAL
LEFT JOIN {{ ref('stg_beefy_db__chain_ids') }} ci ON ci.chain_id = t.chain_id
LEFT JOIN {{ ref('stg_beefy_db__chains') }} c ON lower(trimBoth(c.beefy_name)) = ci.chain_key
-- beefy_db holds glitch rows (2022-06-25 avax, 2023-02-16 fantom, 2023-07-30 ethereum, 2024-04-03 bsc,
-- arbitrum, optimism and ethereum, 2025-04-17 linea...): one chain at 1e9 to 1e20 USD. Summed over every
-- chain, the other rows peak at 1.42e9 (2022-01-15), so no real chain row is above 1.5e9.
WHERE ifNull(tvl_usd, 0) <= {{ to_decimal('1500000000') }}
