{#
  Price observation sources: one select per raw source, writing (series_id, ts, value) for int_price_obs.
  A series is one source's series for one natural key; series_id hashes the source name and that key with
  price_series_id, here and in int_price_series_map, so the map finds a series without a lookup table.
  The selects need no dimension: mapping a series to an asset (and every special case) is a row of the series map.
  {where} is a predicate on the source's time column, from price_obs_window.
#}

{% macro price_series_id(source, k1, k2=none) -%}
  cityHash64('{{ source }}', toString({{ k1 }}){% if k2 is not none %}, toString({{ k2 }}){% endif %})
{%- endmacro %}

{#
  Time window of an int_price_obs run on a source time column:
  - incremental_since (reimport, batched refresh): [since, until)
  - incremental run: the last 3 days (dlt re-reads the head; older late rows come with a reimport)
  - first build without vars: everything
#}
{% macro price_obs_window(col) -%}
  {%- set since = incremental_since_var() -%}
  {%- set until = incremental_until_var() -%}
  {%- if since is not none -%}
    {{ col }} >= toDateTime('{{ since }}', 'UTC'){% if until is not none %} AND {{ col }} < toDateTime('{{ until }}', 'UTC'){% endif %}
  {%- elif is_incremental() -%}
    {{ col }} >= now() - INTERVAL 3 DAY
  {%- else -%}
    1
  {%- endif -%}
{%- endmacro %}

{% macro price_obs_beefy_db() -%}
SELECT
  {{ price_series_id('beefy_db', 'oracle_id') }} AS series_id,
  toDateTime(t, 'UTC') AS ts,
  toFloat64(val) AS value
FROM {{ source('dlt', 'beefy_db___prices') }}
WHERE val IS NOT NULL AND {{ price_obs_window('t') }}
{%- endmacro %}

{# /mootokenprices share price, raw: the old per-1e18-want-units scale is a series map row (scale, scale_until). #}
{% macro price_obs_api() -%}
SELECT
  {{ price_series_id('api', normalize_network_beefy_key('chain_id'), 'moo_token_symbol') }} AS series_id,
  toDateTime(assumeNotNull(date_time), 'UTC') AS ts,
  toFloat64(assumeNotNull(price)) AS value
FROM {{ source('dlt', 'beefy_api___mootokenprices') }}
WHERE price IS NOT NULL AND date_time IS NOT NULL AND moo_token_symbol IS NOT NULL AND {{ price_obs_window('date_time') }}
{%- endmacro %}

{# databarn v1 vault stats: ppfs of a product. #}
{% macro price_obs_dbv1_product() -%}
SELECT
  {{ price_series_id('dbv1_product', 'product_id') }} AS series_id,
  toDateTime(datetime, 'UTC') AS ts,
  toFloat64(assumeNotNull(share_to_underlying_price)) AS value
FROM {{ source('dlt', 'databarn_v1___vault_stats') }}
WHERE share_to_underlying_price > 0 AND {{ price_obs_window('datetime') }}
{%- endmacro %}

{# databarn v1 price feeds, every feed: the series map keeps the share-rate ones (a vault's ppfs), so a feed loaded
   before its import state is not lost. #}
{% macro price_obs_dbv1_feed() -%}
SELECT
  {{ price_series_id('dbv1_feed', 'price_feed_id') }} AS series_id,
  toDateTime(datetime, 'UTC') AS ts,
  toFloat64(price) AS value
FROM {{ source('dlt', 'databarn_v1___prices_raw') }}
WHERE price > 0 AND {{ price_obs_window('datetime') }}
{%- endmacro %}

{# CLM subgraph hourly share price (CLM managers and classic vaults, listed or not). #}
{% macro price_obs_subgraph() -%}
SELECT
  {{ price_series_id('subgraph', 'chain_id', 'product_address') }} AS series_id,
  date_hour AS ts,
  toFloat64(assumeNotNull(share_token_price_usd)) AS value
FROM {{ ref('int_beefy_clm_subgraph__share_price_hourly') }}
WHERE share_token_price_usd IS NOT NULL AND {{ price_obs_window('date_hour') }}
{%- endmacro %}

