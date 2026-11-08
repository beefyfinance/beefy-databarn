-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
    post_hook="{{ price_consumer_done('int_product_stats__share_price_hourly') }}",
  )
}}

{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}
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
  {% if since is not none %}
    {% set threshold = since %}
  {% endif %}
  {% set rp = price_reprice('int_product_stats__share_price_hourly', threshold, 'asset', 'hour') %}
{% endif %}

-- Hourly vault share price: the last 5-minute price of each hour of the product's share token in asset_price_5m
-- (corrections are part of the price series). Products map to their price series through token.asset_id.
-- Incremental on date_hour (1-day lookback), plus the hours a resolver run re-priced (price_reprice: each dirty
-- share asset from the hour of its first changed price). Full refresh and reimport run by month (meta.batch:
-- incremental_since / incremental_until).

SELECT
  t.chain_id AS chain_id,
  t.representation_address AS product_address,
  toStartOfHour(p.date_time) AS date_hour,
  toDecimal256(argMax(p.price, p.date_time), 20) AS share_token_price_usd
FROM {{ ref('asset_price_5m') }} p
INNER JOIN (
  SELECT t.chain_id, t.representation_address, assumeNotNull(t.asset_id) AS asset_id
  FROM {{ ref('token') }} t
  INNER JOIN {{ ref('int_product_keys') }} pk
    ON t.chain_id = pk.chain_id AND t.representation_address = pk.product_address
  WHERE t.asset_id IS NOT NULL
) t ON p.asset_id = t.asset_id
{% if is_incremental() %}
WHERE (
    (p.date_time >= toDateTime('{{ threshold }}') AND p.date_time < now() + INTERVAL 1 DAY)
    OR {{ price_repriced(rp, 'p.asset_id', 'p.date_time') }}
  )
  {% if until is not none %}AND p.date_time < toDateTime('{{ until }}', 'UTC'){% endif %}
{% elif since is not none %}
WHERE p.date_time >= toDateTime('{{ since }}', 'UTC'){% if until is not none %} AND p.date_time < toDateTime('{{ until }}', 'UTC'){% endif %}
{% endif %}
GROUP BY t.chain_id, t.representation_address, date_hour
