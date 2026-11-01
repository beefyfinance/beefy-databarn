{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.datetime as DateTime('UTC')) AS datetime,
  cast(t.investor_id as Int64) AS investor_id,
  cast(t.product_id as Int64) AS product_id,
  cast(t.block_number as Int64) AS block_number,
  {{ evm_transaction_hash('t.transaction_hash') }} AS transaction_hash,
  {{ to_float('t.balance') }} AS balance,
  {{ to_float('t.balance_diff') }} AS balance_diff,
  {{ to_float('t.share_to_underlying_price') }} AS share_to_underlying_price,
  {{ to_float('t.underlying_balance') }} AS underlying_balance,
  {{ to_float('t.underlying_diff') }} AS underlying_diff,
  {{ to_float('t.underlying_to_usd_price') }} AS underlying_to_usd_price,
  {{ to_float('t.usd_balance') }} AS usd_balance,
  {{ to_float('t.usd_diff') }} AS usd_diff,
  {{ to_float('t.pending_rewards') }} AS pending_rewards,
  {{ to_float('t.pending_rewards_diff') }} AS pending_rewards_diff,
  {{ to_float('t.pending_rewards_to_usd_price') }} AS pending_rewards_to_usd_price,
  {{ to_float('t.pending_rewards_usd_balance') }} AS pending_rewards_usd_balance,
  {{ to_float('t.pending_rewards_usd_diff') }} AS pending_rewards_usd_diff
FROM {{ source('dlt', 'databarn_v1___investor_timeline') }} t FINAL
