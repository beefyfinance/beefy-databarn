{{
  config(
    materialized='incremental',
    tags=['intermediate', 'yield'],
    unique_key=['chain_id', 'product_address', 'date_time', 'tx_hash', 'event_idx'],
    order_by=['date_time', 'chain_id', 'product_address'],
    engine='MergeTree',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete+insert',
  )
}}

{% if is_incremental() %}
  {% set threshold_sql %}
    SELECT max(date_time) - INTERVAL 1 DAY FROM {{ this }}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 and threshold_result.rows[0][0] %}
    {% set threshold = threshold_result.rows[0][0] | string %}
  {% else %}
    {% set threshold = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}

-- Intermediate model: Clean and transform harvest events into yield structure
-- This model maps harvest events to a yield model, where underlying_amount_compounded * underlying_token_price_usd represents yield
-- Handles data quality issues, standardizes formats, and prepares for yield aggregation
-- Contains all business logic for yield calculation and filtering

WITH cleaned_yield AS (
  SELECT
    t.txn_timestamp as date_time,
    t.network_id,
    t.vault_beefy_key,
    t.block_number,
    t.txn_idx,
    t.event_idx,
    t.txn_hash as tx_hash,
    t.harvest_amount as underlying_amount_compounded,
    t.want_price as underlying_token_price_usd,
    -- Calculate yield: underlying_amount_compounded * underlying_token_price_usd
    -- Cast result to Decimal256(20) to maintain full precision
    toDecimal256(t.harvest_amount * t.want_price, 20) as underlying_amount_compounded_usd
  FROM {{ ref('stg_beefy_db__harvests') }} t
  {% if is_incremental() %}
    WHERE t.txn_timestamp >= toDateTime('{{ threshold }}') AND t.txn_timestamp < now() + INTERVAL 1 DAY
  {% endif %}
)

-- Output: Clean yield events ready for aggregation
-- Each row represents a yield-generating harvest event
-- Incremental on date_time; only loads new data (with 1-day lookback for late data).
SELECT
  cy.date_time,
  p.chain_id,
  p.product_address,
  cy.block_number,
  cy.txn_idx,
  cy.event_idx,
  cy.tx_hash,
  cy.underlying_amount_compounded,
  cy.underlying_token_price_usd,
  cy.underlying_amount_compounded_usd
FROM cleaned_yield cy
INNER JOIN {{ ref('product') }} p
  ON cy.network_id = p.chain_id
  AND cy.vault_beefy_key = p.beefy_key
WHERE
  -- Filter out invalid timestamps that would convert to 1970-01-01
  cy.date_time IS NOT NULL
  AND toDate(cy.date_time) > '1970-01-01'