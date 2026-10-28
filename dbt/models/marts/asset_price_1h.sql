{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['marts', 'prices'],
    unique_key=['asset_id', 'date_hour'],
    order_by=['asset_id', 'date_hour'],
    partition_by='toYYYYMM(date_hour)',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

-- Hourly OHLC of asset_price_5m (open/close = first/last 5-minute price of the hour).
-- Join facts with: ASOF LEFT JOIN asset_price_1h p ON t.asset_id = p.asset_id AND f.date_hour >= p.date_hour

{% if is_incremental() %}
  {% set max_date_sql %}
    select max(cast(date_hour as Nullable(DateTime('UTC')))) from {{ this }}
  {% endset %}
  {% set max_date_tbl = run_query(max_date_sql) %}
  {% set max_date = max_date_tbl.columns[0][0] %}
  {% if max_date is none %}
    {% set max_date = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}
{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}

SELECT
  asset_id,
  toStartOfHour(date_time) as date_hour,
  argMin(price, date_time) as open,
  max(price) as high,
  min(price) as low,
  argMax(price, date_time) as close,
  cast(avg(price) as Decimal(76, 20)) as avg_price,
  count() as sample_count
FROM {{ ref('asset_price_5m') }}
{% if is_incremental() %}
WHERE
  {% if since is not none %}
  date_time >= toStartOfHour(toDateTime('{{ since }}'))
  {% else %}
  date_time >= toStartOfHour(toDateTime('{{ max_date }}')) - INTERVAL 1 DAY
  {% endif %}
  {% if until is not none %}
  AND date_time < toDateTime('{{ until }}')
  {% endif %}
{% elif since is not none and until is not none %}
WHERE date_time >= toDateTime('{{ since }}') AND date_time < toDateTime('{{ until }}')
{% endif %}
GROUP BY asset_id, date_hour
