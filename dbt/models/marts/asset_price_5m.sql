{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['marts', 'prices'],
    unique_key=['asset_id', 'date_time'],
    order_by=['asset_id', 'date_time'],
    partition_by='toYYYYMM(date_time)',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

-- USD price of assets (price series) in 5-minute slots. Keyed by asset_id, never by oracle key.
-- Tokens link to it with token.asset_id. Join facts with:
--   INNER JOIN token t ON f.chain_id = t.chain_id AND f.token_address = t.representation_address
--   ASOF LEFT JOIN asset_price_5m p ON t.asset_id = p.asset_id AND f.date_time >= p.date_time

{% if is_incremental() %}
  {% set max_date_sql %}
    select max(cast(date_time as Nullable(DateTime('UTC')))) from {{ this }}
  {% endset %}
  {% set max_date_tbl = run_query(max_date_sql) %}
  {% set max_date = max_date_tbl.columns[0][0] %}
  {% if max_date is none %}
    {% set max_date = '1900-01-01 00:00:00' %}
  {% endif %}
{% endif %}
{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}

{% set time_filter %}
{% if is_incremental() %}
WHERE
  {% if since is not none %}
  p.date_time >= toDateTime('{{ since }}')
  {% else %}
  p.date_time >= toDateTime('{{ max_date }}') - INTERVAL 1 DAY
  {% endif %}
  {% if until is not none %}
  AND p.date_time < toDateTime('{{ until }}')
  {% endif %}
{% elif since is not none and until is not none %}
WHERE p.date_time >= toDateTime('{{ since }}') AND p.date_time < toDateTime('{{ until }}')
{% endif %}
{% endset %}

-- beefy_db oracle series
SELECT
  a.asset_id as asset_id,
  p.date_time as date_time,
  p.price as price,
  p.price_source as price_source
FROM {{ ref('int_beefy_oracle_price') }} p FINAL
INNER JOIN (
  SELECT asset_id, assumeNotNull(beefy_price_oracle_key) as beefy_price_oracle_key
  FROM {{ ref('asset') }}
  WHERE asset_source = 'beefy_db_oracle'
) a
  ON p.beefy_price_oracle_key = a.beefy_price_oracle_key
{{ time_filter }}

UNION ALL

-- beefy_api share price series (vault share tokens without beefy_db oracle)
SELECT
  a.asset_id as asset_id,
  p.date_time as date_time,
  p.share_token_price_usd as price,
  p.price_source as price_source
FROM {{ ref('int_product_share_price') }} p FINAL
INNER JOIN (
  SELECT asset_id, assumeNotNull(chain_id) as chain_id, assumeNotNull(share_token_address) as share_token_address
  FROM {{ ref('asset') }}
  WHERE asset_source = 'beefy_api_share'
) a
  ON p.chain_id = a.chain_id
  AND p.product_address = a.share_token_address
{{ time_filter }}
