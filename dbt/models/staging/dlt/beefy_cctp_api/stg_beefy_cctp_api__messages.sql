{{
  config(
    materialized='view',
  )
}}

SELECT
  toInt64OrNull(trim(t.src_network_id)) AS src_network_id,
  cast({{ evm_transaction_hash('t.src_tx_hash') }} as Nullable(String)) AS src_tx_hash,
  cast(t.src_log_index as Int64) AS src_log_index,
  cast(t.updated_at as DateTime('UTC')) AS updated_at,
  cast(t.created_at as DateTime('UTC')) AS created_at,

  toInt64OrNull(trim(t.src_block_number)) AS src_block_number,
  t.src_block_hash AS src_block_hash,
  cast(t.src_block_timestamp as Nullable(String)) AS src_block_timestamp,
  t.src_message AS src_message,
  {{ evm_address('t.src_sender') }} AS src_sender,
  {{ evm_address('t.src_burn_token') }} AS src_burn_token,
  toDecimal256(ifNull(t.src_burn_amount, '0'), 0) / toDecimal256(1000000, 0) AS src_burn_amount_usdc,

  t.attestation_message AS attestation_message,
  t.attestation AS attestation,
  t.attestation_nonce AS attestation_nonce,
  cast(t.attestation_version as Nullable(Int64)) AS attestation_version,
  t.attestation_status AS attestation_status,
  t.lifecycle_state AS lifecycle_state,

  toInt64OrNull(trim(t.dst_network_id)) AS dst_network_id,
  {{ evm_address('t.dst_receiver') }} AS dst_receiver,
  {{ evm_address('t.dst_recipient') }} AS dst_recipient,
  cast({{ evm_transaction_hash('t.dst_tx_hash') }} as Nullable(String)) AS dst_tx_hash,
  {{ evm_address('t.dst_tx_caller') }} AS dst_tx_caller,
  cast(t.dst_log_index as Nullable(Int64)) AS dst_log_index,
  toInt64OrNull(trim(t.dst_block_number)) AS dst_block_number,
  cast(t.dst_block_timestamp as Nullable(String)) AS dst_block_timestamp,
  cast(t.dst_relay_attempts as Nullable(Int64)) AS dst_relay_attempts,
  toBool(ifNull(t.dst_zap_success, false)) AS dst_zap_success,
  toDecimal256(ifNull(t.dst_amount_in, '0'), 0) / toDecimal256(1000000, 0) AS dst_amount_in_usdc,
  toDecimal256(ifNull(t.dst_refunded_amount, '0'), 0) / toDecimal256(1000000, 0) AS dst_refunded_amount_usdc,
  toDecimal256(ifNull(t.dst_recovered_amount, '0'), 0) / toDecimal256(1000000, 0) AS dst_recovered_amount_usdc,

  t.error_code AS error_code,
  t.error_message AS error_message
FROM {{ source('dlt', 'beefy_cctp_api___messages') }} t FINAL

