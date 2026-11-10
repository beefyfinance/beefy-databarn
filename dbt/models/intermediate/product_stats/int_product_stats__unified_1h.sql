-- depends_on: {{ ref('int_price_obs') }}
-- depends_on: {{ ref('int_product_price_assets') }}
{#- The append strategy ignores on_schema_change (dbt-clickhouse inserts with the table's column list): a column
    change needs an ALTER in dbt/.migrations/ before the run. -#}
{{
  config(
    materialized='incremental',
    tags=['intermediate', 'fact_product_stats_1h'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    engine='CoalescingMergeTree',
    on_schema_change='append_new_columns',
    incremental_strategy='append',
    post_hook="{{ price_consumer_done('int_product_stats__unified_1h') }}",
  )
}}

{#- Every value column, with its type: a source branch fills its own columns and NULLs the others.
    No Array columns: CoalescingMergeTree keeps the last non-NULL value of each column, an Array cannot be NULL,
    so the [] of every other source overwrote the arrays of lps_breakdown (fact_product_stats_1h reads them from
    int_product_stats__lps_breakdown_1h instead). Internal names: fact_product_stats_1h renames them. -#}
{% set columns = [
  ('tvl_usd', 'Nullable(Float64)'),
  ('apy', 'Nullable(Float64)'),
  ('compoundings_per_year', 'Nullable(Int64)'),
  ('beefy_performance_fee', 'Nullable(Float64)'),
  ('lp_fee', 'Nullable(Float64)'),
  ('total_apy', 'Nullable(Float64)'),
  ('vault_apr', 'Nullable(Float64)'),
  ('trading_apr', 'Nullable(Float64)'),
  ('clm_apr', 'Nullable(Float64)'),
  ('reward_pool_apr', 'Nullable(Float64)'),
  ('reward_pool_trading_apr', 'Nullable(Float64)'),
  ('vault_apy', 'Nullable(Float64)'),
  ('liquid_staking_apr', 'Nullable(Float64)'),
  ('composable_pool_apr', 'Nullable(Float64)'),
  ('merkl_apr', 'Nullable(Float64)'),
  ('linea_ignition_apr', 'Nullable(Float64)'),
  ('lp_price', 'Nullable(Float64)'),
  ('total_supply', 'Nullable(Decimal(76, 20))'),
  ('underlying_liquidity', 'Nullable(Decimal(76, 20))'),
  ('underlying_price', 'Nullable(Float64)'),
  ('harvest_call_fee', 'Nullable(Decimal(76, 20))'),
  ('harvest_call_fee_usd', 'Nullable(Float64)'),
  ('gas_fee', 'Nullable(Decimal(76, 20))'),
  ('gas_fee_usd', 'Nullable(Float64)'),
  ('platform_fee', 'Nullable(Decimal(76, 20))'),
  ('platform_fee_usd', 'Nullable(Float64)'),
  ('strategist_fee', 'Nullable(Decimal(76, 20))'),
  ('strategist_fee_usd', 'Nullable(Float64)'),
  ('harvest_amount', 'Nullable(Decimal(76, 20))'),
  ('harvest_amount_usd', 'Nullable(Float64)'),
  ('avg_native_price', 'Nullable(Float64)'),
  ('harvest_txn_count', 'Nullable(UInt64)'),
  ('share_token_price_usd', 'Nullable(Float64)'),
  ('want_price_usd', 'Nullable(Float64)'),
] %}

{#- One branch per source. lookback: days re-read before the source's last hour. Snapshot sources re-read a day;
    full-table sources 30 days so a new product's history is not truncated (keys_match tests the same window).
    priced: the source stores prices, so it also re-emits the hours a resolver run re-priced (price_reprice). -#}
{% set sources = [
  {'source': 'tvl', 'model': ref('int_product_stats__tvl_1h'), 'lookback': 1, 'priced': true,
   'columns': ['tvl_usd']},
  {'source': 'apy', 'model': ref('int_product_stats__apy_1h'), 'lookback': 1, 'priced': false,
   'columns': ['apy']},
  {'source': 'apy_breakdown', 'model': ref('int_product_stats__apy_breakdown_1h'), 'lookback': 30, 'priced': false,
   'columns': ['compoundings_per_year', 'beefy_performance_fee', 'lp_fee', 'total_apy', 'vault_apr', 'trading_apr',
               'clm_apr', 'reward_pool_apr', 'reward_pool_trading_apr', 'vault_apy', 'liquid_staking_apr',
               'composable_pool_apr', 'merkl_apr', 'linea_ignition_apr']},
  {'source': 'lps_breakdown', 'model': ref('int_product_stats__lps_breakdown_1h'), 'lookback': 30, 'priced': true,
   'columns': ['lp_price', 'total_supply', 'underlying_liquidity', 'underlying_price']},
  {'source': 'harvest', 'model': ref('int_product_stats__harvest_1h'), 'lookback': 30, 'priced': true,
   'columns': ['harvest_call_fee', 'harvest_call_fee_usd', 'gas_fee', 'gas_fee_usd', 'platform_fee', 'platform_fee_usd',
               'strategist_fee', 'strategist_fee_usd', 'harvest_amount', 'harvest_amount_usd', 'avg_native_price',
               'harvest_txn_count']},
  {'source': 'share_price', 'model': ref('int_product_stats__share_price_1h'), 'lookback': 1, 'priced': true,
   'columns': ['share_token_price_usd']},
  {'source': 'want_price', 'model': ref('int_product_stats__want_price_1h'), 'lookback': 1, 'priced': true,
   'columns': ['want_price_usd']},
] %}

{% set thresholds = {} %}
{% if is_incremental() %}
  {#- One query for every source. A source with no rows yet loads from the beginning: max() of nothing is the epoch,
      and epoch - N days underflows DateTime to 2106, which would skip the source forever
      (int_product_stats__apy_breakdown_1h was empty on prod until November 2026, so its branch never ran). -#}
  {% set threshold_sql %}
    SELECT
    {% for s in sources %}
      toString((SELECT if(count() = 0, toDateTime(0), max(date_hour) - INTERVAL {{ s.lookback }} DAY) FROM {{ this }} WHERE source = '{{ s.source }}')){% if not loop.last %},{% endif %}
    {% endfor %}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% set since = incremental_since_var() %}
  {% for s in sources %}
    {% if since is not none %}
      {% do thresholds.update({s.source: since}) %}
    {% elif execute and threshold_result and threshold_result.rows | length > 0 %}
      {% do thresholds.update({s.source: threshold_result.rows[0][loop.index0] | string}) %}
    {% else %}
      {% do thresholds.update({s.source: '1900-01-01 00:00:00'}) %}
    {% endif %}
  {% endfor %}
  {#- Hours a resolver run re-priced for a product (price_reprice), re-emitted by every priced source. -#}
  {% set rp = price_reprice('int_product_stats__unified_1h', thresholds['tvl'], 'product', 'hour') %}
  {% set repriced = price_repriced(rp, price_product_key('chain_id', 'product_address'), 'date_hour') %}
{% endif %}

-- Materialized intermediate: unified hourly stats from all sources.
-- One row per (chain_id, product_address, date_hour, source) per insert. Append only: CoalescingMergeTree folds
-- all sources of an hour into one row and keeps the latest non-NULL value per column, so a reload simply
-- overwrites. (delete+insert on (..., source) was unsafe: a merged row keeps only one source value, so a
-- harvest reload could delete an hour's tvl/apy that the 1-day branches would never re-insert.)
-- Each source is incremental on its own last hour (thresholds above), so sources progress independently.
SELECT * REPLACE (
  toLowCardinality(product_address) AS product_address,
  toLowCardinality(source) AS source
)
FROM (
{% for s in sources %}
  SELECT
    chain_id,
    product_address,
    date_hour,
    '{{ s.source }}' AS source,
    {% for name, type in columns -%}
    {%- if name in s.columns %}CAST({{ name }} AS {{ type }}){% else %}CAST(NULL AS {{ type }}){% endif %} AS {{ name }}{% if not loop.last %},{% endif %}
    {% endfor %}
  FROM {{ s.model }}
  {% if is_incremental() %}
  WHERE (date_hour >= toDateTime('{{ thresholds[s.source] }}') AND date_hour < now() + INTERVAL 1 DAY)
    {% if s.priced %}OR {{ repriced }}{% endif %}
  {% endif %}
  {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
)
