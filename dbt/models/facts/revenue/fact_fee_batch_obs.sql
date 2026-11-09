{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['revenue'],
    order_by=['txn_timestamp', 'chain_id', 'txn_hash'],
  )
}}

-- One row per fee batch harvest: the protocol fees a chain's fee batcher collected and split between the treasury
-- (revenue) and the reward pool. Amounts are in USD, as beefy_db records them.

SELECT
  fh.txn_timestamp AS txn_timestamp,
  fh.chain_id AS chain_id,
  fh.block_number AS block_number,
  lower(fh.txn_hash) AS txn_hash,
  fh.harvest_usd AS harvest_usd,
  fh.treasury_amt AS treasury_usd,
  fh.rewardpool_amt AS reward_pool_usd,
  fh.native_price AS native_price,
  fh.stable_price AS stable_price
FROM {{ ref('stg_beefy_db__feebatch_harvests') }} fh
