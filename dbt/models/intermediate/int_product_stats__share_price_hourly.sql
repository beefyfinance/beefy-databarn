-- depends_on: {{ ref('int_mootoken_price_corrections') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
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
  {% set since = incremental_since_var() %}
  {% if since is not none %}
    {% set threshold = since %}
  {% endif %}
{% endif %}

-- Hourly vault share price: the last 5-minute share price of each hour from int_product_share_price
-- (corrections already applied there).
-- Incremental on date_hour (1-day lookback), plus every hour touching a mootoken correction so new
-- windows reach old hours. Removing a window needs a reimport.

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(share_token_price_usd, date_time) as share_token_price_usd
FROM {{ ref('int_product_share_price') }} FINAL
{% if is_incremental() %}
WHERE
  (date_time >= toDateTime('{{ threshold }}') and date_time < now() + INTERVAL 1 DAY)
  or (chain_id, product_address, toStartOfHour(date_time)) in (
    select chain_id, product_address, toStartOfHour(date_time) from {{ ref('int_mootoken_price_corrections') }}
  )
{% endif %}
GROUP BY chain_id, product_address, toStartOfHour(date_time)
