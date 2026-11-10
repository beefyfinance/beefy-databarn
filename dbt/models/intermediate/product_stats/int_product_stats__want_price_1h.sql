-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'fact_product_stats_1h'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
    post_hook="{{ price_consumer_done('int_product_stats__want_price_1h') }}",
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
  {% set rp = price_reprice('int_product_stats__want_price_1h', threshold, 'asset', 'hour') %}
{% endif %}

-- Hourly price of the product's want (deposit) token: the last 5-minute price of each hour, in fact_asset_price_5m,
-- of the asset of beefy_wants_price_oracle_key (int_product_price_assets role 'want'). Every product type has one:
-- the LP or token of a classic vault, and of the vault a boost stakes; the manager share of a CLM; the CLM share a
-- reward pool stakes. fact_product_stats_1h.want_price_usd prefers the /lps/breakdown price (lps_breakdown_1h) and
-- falls back on this one.
-- Incremental on date_hour (1-day lookback), plus the hours a resolver run re-priced (price_reprice). Full refresh
-- and reimport run by month (meta.batch: incremental_since / incremental_until), like share_price_1h.
SELECT
  w.chain_id AS chain_id,
  toLowCardinality(w.product_address) AS product_address,
  toStartOfHour(p.date_time) AS date_hour,
  argMax(p.price, p.date_time) AS want_price_usd
FROM {{ ref('fact_asset_price_5m') }} p
INNER JOIN (
  SELECT chain_id, product_address, asset_id
  FROM {{ ref('int_product_price_assets') }}
  WHERE role = 'want'
) w ON p.asset_id = w.asset_id
{% if is_incremental() %}
WHERE (
    (p.date_time >= toDateTime('{{ threshold }}') AND p.date_time < now() + INTERVAL 1 DAY)
    OR {{ price_repriced(rp, 'p.asset_id', 'p.date_time') }}
  )
  {% if until is not none %}AND p.date_time < toDateTime('{{ until }}', 'UTC'){% endif %}
{% elif since is not none %}
WHERE p.date_time >= toDateTime('{{ since }}', 'UTC'){% if until is not none %} AND p.date_time < toDateTime('{{ until }}', 'UTC'){% endif %}
{% endif %}
GROUP BY w.chain_id, w.product_address, date_hour
