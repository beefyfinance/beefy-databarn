{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['marts', 'zap'],
    order_by=['src_txn_timestamp', 'src_chain_id', 'src_txn_hash'],
  )
}}

-- One row per source transaction (src_chain_id, src_txn_hash).
-- Token legs (input / output / zap_fee / refund) are pivoted into parallel arrays.
-- Multi-event txns are merged: USD summed, context via any/anyHeavy.
-- Only Beefy vault zaps: vault_beefy_key is set by the indexer for Beefy product
-- flows. Null-vault rows are third-party protocols calling the Zap Router.

WITH base AS (
  SELECT
    r.network_id as network_id,
    r.block_number as block_number,
    r.txn_idx as txn_idx,
    r.event_idx as event_idx,
    p.txn_timestamp as txn_timestamp,
    toDate(p.txn_timestamp) as txn_date,
    coalesce(r.txn_hash, p.txn_hash) as txn_hash,
    r.caller_address as caller_address,
    r.recipient_address as recipient_address,
    r.target_network_id as target_network_id,
    r.vault_beefy_key as vault_beefy_key,
    r.action as action,
    r.total_usd as total_usd,
    r.cctp_hash as cctp_hash,
    r.swap_source as swap_source,
    p.success as success,
    p.gas_used as gas_used,
    p.native_price as native_price,
    p.from_address as from_address,
    p.to_address as to_address,
    t.zap_type as zap_type,
    t.token_address as token_address,
    po.oracle_id as token_symbol,
    t.token_amount as token_amount,
    t.usd_value as usd_value,
    ifNull(t.event_idx, toInt32(-1)) as transfer_event_idx
  FROM {{ ref('stg_beefy_db__zap_records') }} r
  INNER JOIN {{ ref('stg_beefy_db__zap_parent_transactions') }} p
    ON r.network_id = p.network_id
    AND r.block_number = p.block_number
    AND r.txn_idx = p.txn_idx
  LEFT JOIN {{ ref('stg_beefy_db__zap_token_transfers_v2') }} t
    ON r.network_id = t.network_id
    AND r.block_number = t.block_number
    AND r.txn_idx = t.txn_idx
    AND r.event_idx = t.parent_event_idx
  LEFT JOIN {{ ref('stg_beefy_db__price_oracles') }} po
    ON toString(t.token_id) = po.id
  WHERE r.vault_beefy_key is not null
),

event_metrics AS (
  SELECT
    network_id,
    block_number,
    txn_idx,
    event_idx,
    any(txn_hash) as txn_hash,
    any(txn_timestamp) as txn_timestamp,
    any(txn_date) as txn_date,
    any(caller_address) as caller_address,
    any(recipient_address) as recipient_address,
    any(target_network_id) as target_network_id,
    any(vault_beefy_key) as vault_beefy_key,
    any(action) as action,
    any(total_usd) as total_usd,
    any(cctp_hash) as cctp_hash,
    any(swap_source) as swap_source,
    any(success) as success,
    any(gas_used) as gas_used,
    any(native_price) as native_price,
    any(from_address) as from_address,
    any(to_address) as to_address
  FROM base
  GROUP BY
    network_id,
    block_number,
    txn_idx,
    event_idx
),

event_metrics_signed AS (
  SELECT
    *,
    multiIf(
      lower(action) = 'deposit', total_usd,
      lower(action) = 'withdrawal', -total_usd,
      toDecimal256(0, 20)
    ) as signed_usd
  FROM event_metrics
),

txn_context AS (
  SELECT
    em.network_id as src_chain_id,
    em.txn_hash as src_txn_hash,
    any(em.block_number) as src_block_number,
    any(em.txn_idx) as src_txn_idx,
    any(em.txn_timestamp) as src_txn_timestamp,
    any(em.txn_date) as src_txn_date,
    any(em.caller_address) as caller_address,
    any(em.recipient_address) as recipient_address,
    any(em.target_network_id) as dst_chain_id,
    any(em.vault_beefy_key) as vault_beefy_key,
    anyHeavy(em.action) as action,
    arrayDistinct(groupArray(em.action)) as actions,
    sum(em.total_usd) as total_usd,
    sum(em.signed_usd) as signed_usd,
    any(em.cctp_hash) as cctp_hash,
    any(em.swap_source) as swap_source,
    any(em.success) as success,
    any(em.gas_used) as gas_used,
    any(em.native_price) as native_price,
    any(em.from_address) as from_address,
    any(em.to_address) as to_address,
    uniqExact(em.event_idx) as event_count
  FROM event_metrics_signed em
  GROUP BY
    em.network_id,
    em.txn_hash
),

txn_transfers AS (
  SELECT
    network_id as src_chain_id,
    txn_hash as src_txn_hash,

    arraySort(x -> x.5, groupArrayIf(
      (token_address, ifNull(token_symbol, ''), token_amount, usd_value, transfer_event_idx),
      zap_type = 'input'
    )) as input_legs,
    arraySort(x -> x.5, groupArrayIf(
      (token_address, ifNull(token_symbol, ''), token_amount, usd_value, transfer_event_idx),
      zap_type = 'output'
    )) as output_legs,
    arraySort(x -> x.5, groupArrayIf(
      (token_address, ifNull(token_symbol, ''), token_amount, usd_value, transfer_event_idx),
      zap_type = 'beefy_fee'
    )) as zap_fee_legs,
    arraySort(x -> x.5, groupArrayIf(
      (token_address, ifNull(token_symbol, ''), token_amount, usd_value, transfer_event_idx),
      zap_type = 'refund'
    )) as refund_legs,

    sumIf(usd_value, zap_type = 'input') as input_usd,
    sumIf(usd_value, zap_type = 'output') as output_usd,
    sumIf(usd_value, zap_type = 'beefy_fee') as zap_fee_usd,
    sumIf(usd_value, zap_type = 'refund') as refund_usd
  FROM base
  WHERE zap_type is not null
  GROUP BY
    network_id,
    txn_hash
)

SELECT
  c.src_chain_id as src_chain_id,
  src.chain_name as src_chain_name,
  src.beefy_key as src_chain_beefy_key,
  src.beefy_enabled as src_chain_beefy_enabled,

  c.src_txn_hash as src_txn_hash,
  c.src_block_number as src_block_number,
  c.src_txn_idx as src_txn_idx,
  c.src_txn_timestamp as src_txn_timestamp,
  c.src_txn_date as src_txn_date,

  coalesce(c.dst_chain_id, c.src_chain_id) as dst_chain_id,
  dst.chain_name as dst_chain_name,
  dst.beefy_key as dst_chain_beefy_key,
  dst.beefy_enabled as dst_chain_beefy_enabled,
  toBool(coalesce(c.dst_chain_id, c.src_chain_id) = c.src_chain_id) as is_same_chain_zap,

  c.caller_address as caller_address,
  coalesce(caller.labels, cast([] as Array(String))) as caller_labels,
  coalesce(caller.is_contract, false) as caller_is_contract,
  c.recipient_address as recipient_address,
  coalesce(recipient.labels, cast([] as Array(String))) as recipient_labels,
  coalesce(recipient.is_contract, false) as recipient_is_contract,

  c.vault_beefy_key as vault_beefy_key,
  c.action as action,
  c.actions as actions,
  multiIf(
    lower(c.action) = 'deposit', 'inflow',
    lower(c.action) = 'withdrawal', 'outflow',
    'other'
  ) as flow_direction,

  c.total_usd as total_usd,
  c.signed_usd as signed_usd,
  coalesce(t.input_usd, toDecimal256(0, 20)) as input_usd,
  coalesce(t.output_usd, toDecimal256(0, 20)) as output_usd,
  coalesce(t.zap_fee_usd, toDecimal256(0, 20)) as zap_fee_usd,
  coalesce(t.refund_usd, toDecimal256(0, 20)) as refund_usd,

  arrayMap(x -> x.1, coalesce(t.input_legs, [])) as input_token_addresses,
  arrayMap(x -> x.2, coalesce(t.input_legs, [])) as input_token_symbols,
  arrayMap(x -> x.3, coalesce(t.input_legs, [])) as input_token_amounts,
  arrayMap(x -> x.4, coalesce(t.input_legs, [])) as input_usd_values,

  arrayMap(x -> x.1, coalesce(t.output_legs, [])) as output_token_addresses,
  arrayMap(x -> x.2, coalesce(t.output_legs, [])) as output_token_symbols,
  arrayMap(x -> x.3, coalesce(t.output_legs, [])) as output_token_amounts,
  arrayMap(x -> x.4, coalesce(t.output_legs, [])) as output_usd_values,

  arrayMap(x -> x.1, coalesce(t.zap_fee_legs, [])) as zap_fee_token_addresses,
  arrayMap(x -> x.2, coalesce(t.zap_fee_legs, [])) as zap_fee_token_symbols,
  arrayMap(x -> x.3, coalesce(t.zap_fee_legs, [])) as zap_fee_token_amounts,
  arrayMap(x -> x.4, coalesce(t.zap_fee_legs, [])) as zap_fee_usd_values,

  arrayMap(x -> x.1, coalesce(t.refund_legs, [])) as refund_token_addresses,
  arrayMap(x -> x.2, coalesce(t.refund_legs, [])) as refund_token_symbols,
  arrayMap(x -> x.3, coalesce(t.refund_legs, [])) as refund_token_amounts,
  arrayMap(x -> x.4, coalesce(t.refund_legs, [])) as refund_usd_values,

  c.success as success,
  c.gas_used as gas_used,
  c.native_price as native_price,
  c.from_address as from_address,
  c.to_address as to_address,
  c.cctp_hash as cctp_hash,
  c.swap_source as swap_source,
  c.event_count as event_count
FROM txn_context c
LEFT JOIN txn_transfers t
  ON c.src_chain_id = t.src_chain_id
  AND c.src_txn_hash = t.src_txn_hash
LEFT JOIN {{ ref('chain') }} src
  ON c.src_chain_id = src.chain_id
LEFT JOIN {{ ref('chain') }} dst
  ON coalesce(c.dst_chain_id, c.src_chain_id) = dst.chain_id
LEFT JOIN {{ ref('account') }} caller
  ON c.caller_address = caller.address
LEFT JOIN {{ ref('account') }} recipient
  ON c.recipient_address = recipient.address
