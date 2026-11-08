-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['marts', 'prices'],
    unique_key=['asset_id', 'date_day'],
    order_by=['asset_id', 'date_day'],
    partition_by='toYear(date_day)',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    pre_hook="{{ price_rollup_clear_pending('asset_price_1d') }}",
    post_hook="{{ price_consumer_done('asset_price_1d') }}",
  )
}}

-- Daily (UTC) OHLC of asset_price_5m. Small enough for an ASOF join: ASOF LEFT JOIN asset_price_1d p
-- ON p.asset_id = f.asset_id AND toDate(f.ts) >= p.date_day.
-- Each run rebuilds the days of the resolver runs it has not applied yet: their head range, and each dirty asset
-- from the day its recompute started (macros/prices/price_runs.sql). The pre-hook deletes those days first.

{% set pending = price_runs_pending('asset_price_1d') %}

SELECT
  asset_id,
  toDate(date_time) AS date_day,
  argMin(price, date_time) AS open,
  max(price) AS high,
  min(price) AS low,
  argMax(price, date_time) AS close,
  avg(price) AS avg_price,
  toUInt32(count()) AS sample_count
FROM {{ ref('asset_price_5m') }}
{% if is_incremental() %}
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
