{{
  config(
    materialized='incremental',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    engine='CoalescingMergeTree',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

{% if is_incremental() %}
  {% set threshold_sql %}
    SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 and threshold_result.rows[0][0] %}
    {% set threshold = threshold_result.rows[0][0] | string %}
  {% else %}
    {% set threshold = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}

-- Materialized intermediate: Hourly tvl aggregations
-- Incremental on date_hour; only loads hours we don't already have (with 1-day lookback for late data).
-- Combines tvl staging data with product information and aggregates to hourly

WITH tvl_with_product AS (
  SELECT
    p.chain_id,
    p.product_address,
    a.date_time,
    a.tvl_usd
  FROM {{ ref('stg_beefy_db__tvls') }} a
  INNER JOIN {{ ref('stg_beefy_db__vault_ids') }} vi
    ON a.vault_id = vi.vault_id
  INNER JOIN {{ ref('product') }} p
    ON vi.beefy_key = p.beefy_key
  WHERE
    a.tvl_usd between 0 and 1000000000
    {% if is_incremental() %}
    AND toStartOfHour(a.date_time) >= toDateTime('{{ threshold }}')
    AND toStartOfHour(a.date_time) < now() + INTERVAL 1 DAY
    {% endif %}
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(tvl_usd, date_time) as tvl_usd
FROM tvl_with_product
GROUP BY chain_id, product_address, toStartOfHour(date_time)