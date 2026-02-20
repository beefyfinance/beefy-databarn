{{
  config(
    materialized='incremental',
    tags=['intermediate', 'yield'],
    unique_key=['chain_id', 'product_address', 'txn_timestamp', 'txn_hash', 'event_idx'],
    order_by=['txn_timestamp', 'chain_id', 'product_address'],
    engine='MergeTree',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete+insert',
  )
}}

{% if is_incremental() %}
  {% set threshold_sql %}
    SELECT max(txn_timestamp) - INTERVAL 1 DAY FROM {{ this }}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 and threshold_result.rows[0][0] %}
    {% set threshold = threshold_result.rows[0][0] | string %}
  {% else %}
    {% set threshold = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}


SELECT
  h.txn_timestamp as txn_timestamp,
  p.chain_id,
  p.product_address,
  h.block_number,
  h.txn_idx,
  h.event_idx,
  h.txn_hash as txn_hash,
  h.harvest_amount as underlying_amount_compounded,
  h.want_price as underlying_token_price_usd,
  toDecimal256(h.harvest_amount * h.want_price, 20) as underlying_amount_compounded_usd
FROM {{ ref('stg_beefy_db__harvests') }} h
INNER JOIN {{ ref('product') }} p
  ON h.network_id = p.chain_id
  AND h.vault_beefy_key = p.beefy_key
WHERE
  h.txn_timestamp IS NOT NULL
  AND toDate(h.txn_timestamp) > '1970-01-01'
  {% if is_incremental() %}
  AND h.txn_timestamp >= toDateTime('{{ threshold }}')
  AND h.txn_timestamp < now() + INTERVAL 1 DAY
  {% endif %}