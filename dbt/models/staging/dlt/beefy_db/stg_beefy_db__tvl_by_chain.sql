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
WHERE NOT (
  t.chain_id = 1 AND
  date_time BETWEEN '2024-04-03' AND '2024-04-04'
  AND tvl_usd > {{ to_decimal('8000000000') }}
)
