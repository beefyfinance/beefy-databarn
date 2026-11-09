-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['prices'],
    unique_key=['asset_id', 'date_day'],
    order_by=['asset_id', 'date_day'],
    partition_by='toYear(date_day)',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    pre_hook="{{ price_rollup_clear_pending('fact_asset_price_1d') }}",
    post_hook="{{ price_consumer_done('fact_asset_price_1d') }}",
  )
}}

-- Daily (UTC) OHLC of fact_asset_price_5m. Small enough for an ASOF join: ASOF LEFT JOIN fact_asset_price_1d p
-- ON p.asset_id = f.asset_id AND toDate(f.ts) >= p.date_day.
-- Each run rebuilds the days of the resolver runs it has not applied yet: their head range, and each dirty asset
-- from the day its recompute started (macros/prices/price_runs.sql). The pre-hook deletes those days first.
-- A reimport (incremental_since, with incremental_until in monthly batches) rebuilds the days of its window instead,
-- and marks the pending runs applied like the other priced models.

{% set pending = price_runs_pending('fact_asset_price_1d') %}
{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}

SELECT
  asset_id,
  toDate(date_time) AS date_day,
  argMin(price, date_time) AS open,
  max(price) AS high,
  min(price) AS low,
  argMax(price, date_time) AS close,
  avg(price) AS avg_price,
  toUInt32(count()) AS sample_count
FROM {{ ref('fact_asset_price_5m') }}
{% if since is not none %}
WHERE date_time >= toStartOfDay(toDateTime('{{ since }}', 'UTC'))
  {% if until is not none %}AND date_time < toStartOfDay(toDateTime('{{ until }}', 'UTC')){% endif %}
{% elif is_incremental() %}
WHERE
  {% if pending['any'] %}
  (date_time >= toStartOfDay({{ pending['head_from'] }}) AND date_time < {{ pending['head_to'] }})
  OR (asset_id, toStartOfDay(date_time)) IN (
    SELECT asset_id, arrayJoin(arrayMap(i -> from_day + toIntervalDay(i), range(toUInt32(dateDiff('day', from_day, {{ pending['head_to'] }})) + 1)))
    FROM ({{ pending['dirty'] }})
  )
  {% else %}
  0
  {% endif %}
{% endif %}
GROUP BY asset_id, date_day
