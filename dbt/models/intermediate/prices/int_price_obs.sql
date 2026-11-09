{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    engine='ReplacingMergeTree(ingested_at)',
    order_by=['series_id', 'ts'],
    partition_by='toYear(ts)',
    tags=['intermediate', 'prices'],
  )
}}

-- Every raw price value, keyed by source series (see macros/prices/price_sources.sql). The resolver
-- (fact_asset_price_5m) maps series to assets with int_price_series_map.
-- Incremental runs re-read the last 3 days of each source and insert only rows not already here, so a run writes
-- only new or changed values: the resolver recomputes from what each run wrote (ingested_at above its last mark).
-- A reimport (make dbt reimport int_price_obs+ <since>, batched by month) inserts its range whole: the resolver
-- recomputes from the reimport start. The newest row of a (series_id, ts) wins (ReplacingMergeTree on ingested_at,
-- and the resolver reads argMax by it, so no FINAL is needed).
-- Year partitions: a reimport block spanning many months writes one part per partition (at most 100 per block).
-- A new source's history: a reimport restricted to it (var price_obs_sources, below).

{% set dedupe = is_incremental() and incremental_since_var() is none %}

{#- var price_obs_sources: only these sources (backfill of a new source: with incremental_since, e.g.
    --vars '{"incremental_since": "2020-01-01", "price_obs_sources": ["subgraph"]}'). -#}
{% set all_sources = ['beefy_db', 'api', 'dbv1_product', 'dbv1_feed', 'subgraph'] %}
{% set sources = var('price_obs_sources', all_sources) %}
{% for src in sources %}{% if src not in all_sources %}{{ exceptions.raise_compiler_error('price_obs_sources: unknown source ' ~ src) }}{% endif %}{% endfor %}

WITH new AS (
  {%- for src in sources %}
  {% if not loop.first %}UNION ALL{% endif %}
  {{ context['price_obs_' ~ src]() }}
  {%- endfor %}
)
SELECT series_id, ts, assumeNotNull(value) AS value, now64(3, 'UTC') AS ingested_at
FROM new
{% if dedupe %}
WHERE (series_id, ts, value) NOT IN (
  SELECT series_id, ts, value FROM {{ this }} WHERE ts >= now() - INTERVAL 3 DAY - INTERVAL 1 HOUR
)
{% endif %}
