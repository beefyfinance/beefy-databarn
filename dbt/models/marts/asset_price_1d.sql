{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['marts', 'prices'],
    unique_key=['asset_id', 'date_day'],
    order_by=['asset_id', 'date_day'],
    partition_by='toYYYYMM(date_day)',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

-- Daily OHLC (UTC days) of asset_price_1h. avg_price is weighted by 5-minute sample count.
-- Join facts with: ASOF LEFT JOIN asset_price_1d p ON t.asset_id = p.asset_id AND f.date_day >= p.date_day

{% if is_incremental() %}
  {% set max_date_sql %}
    select max(cast(date_day as Nullable(Date))) from {{ this }}
  {% endset %}
  {% set max_date_tbl = run_query(max_date_sql) %}
  {% set max_date = max_date_tbl.columns[0][0] %}
  {% if max_date is none %}
    {% set max_date = '1900-01-01' %}
  {% endif %}
{% endif %}
{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}

SELECT
  h.asset_id as asset_id,
  toDate(h.date_hour) as date_day,
  argMin(h.open, h.date_hour) as open,
  max(h.high) as high,
  min(h.low) as low,
  argMax(h.close, h.date_hour) as close,
  cast(sum(h.avg_price * h.sample_count) / sum(h.sample_count) as Decimal(76, 20)) as avg_price,
  sum(h.sample_count) as sample_count
FROM {{ ref('asset_price_1h') }} h
{% if is_incremental() %}
WHERE
  {% if since is not none %}
  h.date_hour >= toStartOfDay(toDateTime('{{ since }}'))
  {% else %}
  h.date_hour >= toDateTime(toDate('{{ max_date }}')) - INTERVAL 1 DAY
  {% endif %}
  {% if until is not none %}
  AND h.date_hour < toDateTime('{{ until }}')
  {% endif %}
{% elif since is not none and until is not none %}
WHERE h.date_hour >= toDateTime('{{ since }}') AND h.date_hour < toDateTime('{{ until }}')
{% endif %}
GROUP BY h.asset_id, date_day
