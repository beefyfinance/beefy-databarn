{{
  config(
    materialized='view',
    tags=['marts', 'zap', 'zap_v4'],
  )
}}

SELECT
  m.src_network_id,
  src.chain_name AS source_chain_name,
  src.beefy_key AS source_chain_beefy_key,

  m.src_tx_hash,
  m.src_log_index,

  m.created_at,
  m.updated_at,

  m.src_block_number,
  m.src_block_hash,
  m.src_block_timestamp AS src_block_timestamp_raw,
  coalesce(parseDateTimeBestEffortOrNull(m.src_block_timestamp), m.created_at) AS src_block_datetime,
  toDate(coalesce(parseDateTimeBestEffortOrNull(m.src_block_timestamp), m.created_at)) AS src_block_date,

  m.src_message,
  m.src_sender,
  m.src_burn_token,
  m.src_burn_amount_usdc,

  m.attestation_message,
  m.attestation,
  m.attestation_nonce,
  m.attestation_version,
  m.attestation_status,
  m.lifecycle_state,

  m.dst_network_id,
  dst.chain_name AS target_chain_name,
  dst.beefy_key AS target_chain_beefy_key,

  m.dst_receiver,
  m.dst_recipient,
  m.dst_tx_hash,
  m.dst_tx_caller,
  m.dst_log_index,
  m.dst_block_number,
  m.dst_block_timestamp AS dst_block_timestamp_raw,
  parseDateTimeBestEffortOrNull(m.dst_block_timestamp) AS dst_block_datetime,
  m.dst_relay_attempts,
  m.dst_zap_success,
  m.dst_amount_in_usdc,
  m.dst_refunded_amount_usdc,
  m.dst_recovered_amount_usdc,

  m.error_code,
  m.error_message
FROM {{ ref('stg_beefy_cctp_api__messages') }} m
LEFT JOIN {{ ref('chain') }} src
  ON m.src_network_id = src.chain_id
LEFT JOIN {{ ref('chain') }} dst
  ON m.dst_network_id = dst.chain_id

