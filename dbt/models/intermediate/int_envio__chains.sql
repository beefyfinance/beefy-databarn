{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

-- Intermediate model: All chains covered by envio
-- Extracts distinct network_ids from token balances that are associated with accounts
-- This represents all chains where envio tracks account data

SELECT DISTINCT
  tb.network_id AS network_id,
  c.chain_id AS chain_id
FROM {{ ref('stg_envio__token_balance') }} tb
INNER JOIN {{ ref('stg_envio__account') }} a
  ON tb.account_id = a.id
INNER JOIN {{ ref('int_chain_keys') }} c
  ON tb.network_id = c.network_id

