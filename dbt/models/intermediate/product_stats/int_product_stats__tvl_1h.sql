-- depends_on: {{ ref('int_price_obs') }}
-- depends_on: {{ ref('int_product_price_assets') }}
{{
  config(
    materialized='incremental',
    tags=['intermediate', 'fact_product_stats_1h'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    engine='CoalescingMergeTree',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    post_hook="{{ price_consumer_done('int_product_stats__tvl_1h') }}",
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
  {% set rp = price_reprice('int_product_stats__tvl_1h', threshold, 'product', 'hour') %}
{% endif %}
{#- Hours to (re)compute, every source of them: the incremental window, plus the hours a resolver run re-priced for
    a product (price_reprice). window(product chain, product address, hour) per source. -#}
{% set window = {} %}
{% for name, c, a, h in [('beefy_db', 'p.chain_id', 'p.product_address', 'toStartOfHour(a.date_time)'), ('dbv1', 'chain_id', 'product_address', 'date_hour'), ('clm', 'chain_id', 'product_address', 'date_hour')] %}
  {% set upper = (' and ' ~ h ~ " < toDateTime('" ~ until ~ "')") if until is not none else '' %}
  {% if is_incremental() %}
    {% do window.update({name: '((' ~ h ~ " >= toDateTime('" ~ threshold ~ "') and " ~ h ~ ' < now() + INTERVAL 1 DAY) or '
        ~ price_repriced(rp, price_product_key(c, a), h) ~ ')' ~ upper}) %}
  {% elif since is not none %}
    {#- First batch of a batched full refresh: only that window. -#}
    {% do window.update({name: '(' ~ h ~ " >= toDateTime('" ~ since ~ "')" ~ upper ~ ')'}) %}
  {% else %}
    {% do window.update({name: '1'}) %}
  {% endif %}
{% endfor %}

-- Materialized intermediate: Hourly tvl aggregations
-- Incremental on date_hour; only loads hours we don't already have (with 1-day lookback for late data).
-- Combines tvl staging data with product information and aggregates to hourly, then applies
-- int_product_tvl_corrections (beefy-db TVL is computed with its own, possibly mispriced, prices).
-- Hours beefy-db missed fall back on databarn v1 staked want * want price, or on the CLM subgraph.
-- The databarn fallback is priced with fact_asset_price_5m: hours a resolver run re-priced are recomputed (all sources).

WITH tvl_with_product AS (
  SELECT
    p.chain_id,
    p.product_address,
    a.date_time,
    a.tvl_usd
  FROM {{ ref('stg_beefy_db__tvls') }} a
  INNER JOIN {{ ref('stg_beefy_db__vault_ids') }} vi
    ON a.vault_id = vi.vault_id
  INNER JOIN {{ ref('int_product_keys') }} p
    ON vi.beefy_key = p.beefy_key
  WHERE
    a.tvl_usd between 0 and 1000000000
    AND {{ window['beefy_db'] }}
)

-- Hourly rows from int_product_tvl_corrections (priority 1) override the raw hourly TVL (priority 0),
-- which overrides databarn v1 and the CLM subgraph (priority -1).
-- All corrected rows are always re-emitted so incremental runs refresh them.
SELECT
  chain_id,
  toLowCardinality(product_address) AS product_address,
  date_hour,
  argMax(tvl_usd, (priority, date_time)) as tvl_usd
FROM (
  SELECT chain_id, product_address, toStartOfHour(date_time) as date_hour, date_time, tvl_usd, 0 as priority
  FROM tvl_with_product
  UNION ALL
  SELECT chain_id, product_address, date_hour, date_hour as date_time, tvl_usd, 1 as priority
  FROM {{ ref('int_product_tvl_corrections') }}
  UNION ALL
  SELECT chain_id, product_address, date_hour, date_hour as date_time, staked_underlying * want_price_usd as tvl_usd, -1 as priority
  FROM {{ ref('int_databarn_v1__vault_stats_1h') }}
  WHERE staked_underlying * want_price_usd between 0 and 1000000000
    AND {{ window['dbv1'] }}
  UNION ALL
  SELECT chain_id, product_address, date_hour, date_hour as date_time, assumeNotNull(tvl_usd) as tvl_usd, -1 as priority
  FROM {{ ref('int_beefy_clm_subgraph__clm_stats_1h') }}
  WHERE tvl_usd between 0 and 1000000000
    AND {{ window['clm'] }}
)
GROUP BY chain_id, product_address, date_hour
-- Stops 3 months after the product retires or its chain goes eol (int_product_stats__tracking).
HAVING {{ product_is_tracked() }}
