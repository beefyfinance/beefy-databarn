{#
  SQL of the price resolver (materialization price_resolver, model asset_price_5m). t: relation names (see
  price_resolver_tables). lo/hi: slot range [lo, hi) as SQL DateTime expressions. flt: SQL predicate on asset_id ('1' for every asset). kind: 'oracle' or 'share'.
#}

{% macro price_resolver_tables(target) %}
  {%- set obs = ref('int_price_obs') -%}
  {%- set s = obs.schema -%}
  {{ return({
    'target': target,
    'obs': obs,
    'map': ref('int_price_series_map'),
    'corr_oracle': ref('int_price_oracle_corrections'),
    'corr_share': ref('int_mootoken_price_corrections'),
    'state': s ~ '.int_price_asset_state',
    'bounds': s ~ '.int_price_asset_bounds',
    'runs': s ~ '.int_price_runs',
    'map_applied': s ~ '.int_price_map_applied',
    'corr_applied': s ~ '.int_price_corrections_applied',
    'slots': s ~ '.int_price_scratch_slots',
    'states': s ~ '.int_price_scratch_states',
    'assets': s ~ '.int_price_scratch_assets',
  }) }}
{% endmacro %}

{# Spill rather than fail: memory of a step grows with assets x slots of its range. #}
{% macro price_resolver_settings() -%}
SETTINGS max_bytes_before_external_group_by = 1000000000, max_bytes_before_external_sort = 1000000000
{%- endmacro %}

{# Correction rows, as observations of their correction series (always the newest: priority 90). #}
{% macro price_corrections_select(t) -%}
SELECT {{ price_series_id('corr_oracle', 'beefy_price_oracle_key') }} AS series_id, date_time AS ts, toFloat64(price) AS value
FROM {{ t.corr_oracle }}
UNION ALL
SELECT {{ price_series_id('corr_share', 'chain_id', 'product_address') }}, date_time, toFloat64(share_token_price_usd)
FROM {{ t.corr_share }}
{%- endmacro %}

{# First slot and price of every oracle asset (the want of a vault can start after the vault's first ppfs): one row
   per resolved range, merged by the engine. #}
{% macro price_bounds_out(t, lo) -%}
INSERT INTO {{ t.bounds }}
SELECT asset_id, min(slot), argMinState(value, slot) FROM {{ t.slots }} WHERE slot >= {{ lo }} GROUP BY asset_id
{%- endmacro %}

{% macro price_resolver_ddl(t) %}
  {%- set statements = [
    "CREATE TABLE IF NOT EXISTS " ~ t.target ~ " (
      asset_id UInt64,
      date_time DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
      price Float64 CODEC(Gorilla, ZSTD(1)),
      price_source LowCardinality(String),
      is_carried UInt8
    ) ENGINE = MergeTree PARTITION BY toYYYYMM(date_time) ORDER BY (asset_id, date_time)",
    "CREATE TABLE IF NOT EXISTS " ~ t.bounds ~ " (
      asset_id UInt64,
      first_slot SimpleAggregateFunction(min, DateTime('UTC')),
      first_price AggregateFunction(argMin, Float64, DateTime('UTC'))
    ) ENGINE = AggregatingMergeTree ORDER BY asset_id",
    "CREATE TABLE IF NOT EXISTS " ~ t.state ~ " (
      asof DateTime('UTC'),
      asset_id UInt64,
      slot DateTime('UTC'),
      usd Float64,
      ppfs Float64,
      source LowCardinality(String),
      want_asset_id UInt64
    ) ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(asof) ORDER BY (asof, asset_id)
    TTL asof + INTERVAL 2 DAY DELETE WHERE asof != toStartOfDay(asof)",
    "CREATE TABLE IF NOT EXISTS " ~ t.runs ~ " (
      run_id UInt64,
      ran_at DateTime64(3, 'UTC'),
      kind LowCardinality(String),
      watermark DateTime('UTC'),
      obs_mark DateTime64(3, 'UTC'),
      head_from DateTime('UTC'),
      head_to DateTime('UTC'),
      dirty Array(Tuple(asset_id UInt64, from_day DateTime('UTC'), from_ts DateTime('UTC')))
    ) ENGINE = MergeTree ORDER BY run_id",
    "CREATE TABLE IF NOT EXISTS " ~ t.map_applied ~ " AS " ~ t.map,
    "CREATE TABLE IF NOT EXISTS " ~ t.corr_applied ~ " (series_id UInt64, ts DateTime('UTC'), value Float64)
      ENGINE = MergeTree ORDER BY (series_id, ts)",
    "CREATE TABLE IF NOT EXISTS " ~ t.slots ~ " (
      asset_id UInt64, slot DateTime('UTC'), value Float64, obs_kind Enum8('usd' = 1, 'ppfs' = 2),
      source LowCardinality(String), want_asset_id UInt64
    ) ENGINE = MergeTree ORDER BY (asset_id, slot)",
    "CREATE TABLE IF NOT EXISTS " ~ t.states ~ " (
      asset_id UInt64, slot DateTime('UTC'), usd Float64, ppfs Float64, source LowCardinality(String), want_asset_id UInt64
    ) ENGINE = MergeTree ORDER BY (asset_id, slot)",
    "CREATE TABLE IF NOT EXISTS " ~ t.assets ~ " (asset_id UInt64, from_day DateTime('UTC'), from_ts DateTime('UTC'))
      ENGINE = MergeTree ORDER BY asset_id",
  ] -%}
  {{ return(statements) }}
{% endmacro %}

{# Per slot, the highest-priority value of the assets of one kind; a fallback (priority < fallback) only after an
   hour with no primary value. Observations: int_price_obs plus the correction rows. #}
{% macro price_stage_slots(t, kind, lo, hi, flt, fallback=20) -%}
INSERT INTO {{ t.slots }}
WITH
  m AS (
    SELECT series_id, source, asset_id, obs_kind, priority, want_asset_id, ppfs_log10, scale, scale_until, valid_from, valid_to
    FROM {{ t.map }}
    WHERE asset_kind = '{{ kind }}' AND ({{ flt }})
  ),
  raw AS (
    SELECT series_id, ts, value, ingested_at FROM {{ t.obs }}
    WHERE series_id IN (SELECT series_id FROM m) AND ts >= {{ lo }} - INTERVAL 1 HOUR AND ts < {{ hi }}
    UNION ALL
    SELECT series_id, ts, value, toDateTime64(0, 3, 'UTC') AS ingested_at FROM ({{ price_corrections_select(t) }})
    WHERE series_id IN (SELECT series_id FROM m) AND ts >= {{ lo }} - INTERVAL 1 HOUR AND ts < {{ hi }}
  ),
  obs AS (
    SELECT
      m.asset_id AS asset_id,
      toStartOfFiveMinutes(o.ts) AS slot,
      o.ts AS ts,
      o.ingested_at AS ingested_at,
      multiIf(
        m.obs_kind = 'usd', o.value * if(o.ts < m.scale_until, m.scale, 1),
        -- databarn v1 ppfs exactly 1e12 off its expected scale (yei, joe-auto, compound-op, first-day rows)
        abs(log10(o.value) - m.ppfs_log10 + 12) < 1, o.value * 1e12,
        abs(log10(o.value) - m.ppfs_log10 - 12) < 1, o.value / 1e12,
        o.value
      ) AS v,
      m.obs_kind AS obs_kind,
      m.priority AS priority,
      m.source AS src,
      m.want_asset_id AS want_asset_id,
      o.series_id AS series_id
    FROM raw o
    INNER JOIN m ON o.series_id = m.series_id
    WHERE o.ts >= m.valid_from AND o.ts < m.valid_to
      AND (m.obs_kind = 'usd' OR o.value > 0)
  ),
  per_slot AS (
    SELECT
      asset_id,
      slot,
      max(priority) AS top,
      -- Same priority and time: the source name decides, then the newest row, then the series and value (two feeds
      -- of one vault at the same second), so every build picks the same value.
      argMax(v, (priority, ts, src, ingested_at, series_id, v)) AS value,
      argMax(obs_kind, (priority, ts, src, ingested_at, series_id, v)) AS obs_kind,
      argMax(src, (priority, ts, src, ingested_at, series_id, v)) AS source,
      argMax(want_asset_id, (priority, ts, src, ingested_at, series_id, v)) AS want_asset_id
    FROM obs
    GROUP BY asset_id, slot
  )
SELECT asset_id, slot, value, obs_kind, source, want_asset_id
FROM (
  SELECT
    *,
    max(if(top >= {{ fallback }}, slot, toDateTime(0, 'UTC')))
      OVER (PARTITION BY asset_id ORDER BY slot ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_primary
  FROM per_slot
)
WHERE top >= {{ fallback }} OR slot >= last_primary + INTERVAL 1 HOUR
{{ price_resolver_settings() }}
{%- endmacro %}

{# Oracle assets: each value expanded forward up to the next value, at most 1 day. #}
{% macro price_stage1_write(t, lo, hi) -%}
INSERT INTO {{ t.target }}
SELECT
  asset_id,
  toDateTime(arrayJoin(range(
    toUInt32(greatest(slot, {{ lo }})),
    toUInt32(least(next_slot, slot + INTERVAL 1 DAY, {{ hi }})),
    300
  )), 'UTC') AS date_time,
  value,
  source,
  toUInt8(date_time != slot)
FROM (
  SELECT
    asset_id, slot, value, source,
    leadInFrame(slot, 1, toDateTime('2100-01-01 00:00:00', 'UTC'))
      OVER (PARTITION BY asset_id ORDER BY slot ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_slot
  FROM {{ t.slots }}
)
{{ price_resolver_settings() }}
{%- endmacro %}

{# Share assets, step 1: each slot's value as usd and as ppfs, through the want price of the slot. A databarn ppfs
   before its want oracle's first price takes that first price, at most 7 days ahead. #}
{% macro price_stage2_states(t, lo, hi) -%}
INSERT INTO {{ t.states }}
WITH
  wants AS (SELECT DISTINCT want_asset_id FROM {{ t.slots }} WHERE want_asset_id != 0),
  first_want AS (
    SELECT asset_id, min(first_slot) AS first_slot, argMinMerge(first_price) AS price
    FROM {{ t.bounds }}
    WHERE asset_id IN (SELECT want_asset_id FROM wants)
    GROUP BY asset_id
  )
SELECT
  s.asset_id,
  s.slot,
  multiIf(s.obs_kind = 'usd', s.value, s.value * want) AS usd,
  multiIf(s.obs_kind = 'ppfs', s.value, want > 0, s.value / want, nan) AS ppfs,
  s.source,
  s.want_asset_id
FROM (
  SELECT
    s.*,
    multiIf(
      w.price > 0, w.price,
      s.obs_kind = 'ppfs' AND f.first_slot > s.slot AND f.first_slot < s.slot + INTERVAL 7 DAY, f.price,
      0
    ) AS want
  FROM {{ t.slots }} s
  LEFT JOIN (
    SELECT asset_id, date_time, price FROM {{ t.target }}
    WHERE asset_id IN (SELECT want_asset_id FROM wants)
      AND date_time >= {{ lo }} - INTERVAL 1 HOUR AND date_time < {{ hi }}
  ) w ON s.want_asset_id = w.asset_id AND s.slot = w.date_time
  LEFT JOIN first_want f ON s.want_asset_id = f.asset_id
) s
WHERE NOT (s.obs_kind = 'ppfs' AND want = 0)
  AND usd < 1e30
{{ price_resolver_settings() }}
{%- endmacro %}

{# Share assets, step 2: every slot: its own value, else the last ppfs (any age) x the want price of the slot, else
   the last usd value at most 1 day old. #}
{% macro price_stage2_write(t, lo, hi) -%}
INSERT INTO {{ t.target }}
SELECT asset_id, date_time, price, source, is_carried
FROM (
  SELECT
    e.asset_id AS asset_id,
    e.date_time AS date_time,
    multiIf(
      e.date_time = e.slot, e.usd,
      NOT isNaN(e.ppfs) AND w.price > 0, e.ppfs * w.price,
      isNaN(e.ppfs), e.usd,
      nan
    ) AS price,
    e.source AS source,
    toUInt8(e.date_time != e.slot) AS is_carried
  FROM (
    SELECT
      asset_id, slot, usd, ppfs, source, want_asset_id,
      toDateTime(arrayJoin(range(
        toUInt32(greatest(slot, {{ lo }})),
        toUInt32(least(next_slot, if(isNaN(ppfs), slot + INTERVAL 1 DAY, toDateTime('2100-01-01 00:00:00', 'UTC')), {{ hi }})),
        300
      )), 'UTC') AS date_time
    FROM (
      SELECT
        *,
        leadInFrame(slot, 1, toDateTime('2100-01-01 00:00:00', 'UTC'))
          OVER (PARTITION BY asset_id ORDER BY slot ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_slot
      FROM {{ t.states }}
    )
  ) e
  LEFT JOIN (
    SELECT asset_id, date_time, price FROM {{ t.target }}
    WHERE asset_id IN (SELECT DISTINCT want_asset_id FROM {{ t.states }} WHERE want_asset_id != 0)
      AND date_time >= {{ lo }} AND date_time < {{ hi }}
  ) w ON e.want_asset_id = w.asset_id AND e.date_time = w.date_time
)
WHERE NOT isNaN(price) AND price < 1e30
{{ price_resolver_settings() }}
{%- endmacro %}

{# Carry state in: the last value of each asset as of lo (written by the range or run that ended at lo, or the day
   state of lo). #}
{% macro price_carry_in(t, kind, lo, flt) -%}
{%- if kind == 'oracle' -%}
INSERT INTO {{ t.slots }}
SELECT asset_id, slot, usd, 'usd', source, want_asset_id
{%- else -%}
INSERT INTO {{ t.states }}
SELECT asset_id, slot, usd, ppfs, source, want_asset_id
{%- endif %}
FROM {{ t.state }} FINAL
WHERE asof = {{ lo }}
  AND asset_id IN (SELECT asset_id FROM {{ t.map }} WHERE asset_kind = '{{ kind }}' AND ({{ flt }}))
{%- endmacro %}

{# Carry state out: the last value of each asset as of hi, and as of every day start inside (lo, hi), so a later run
   can recompute from the start of any day. Oracle values carry 1 day; a share keeps its ppfs at any age. #}
{% macro price_state_out(t, kind, lo, hi) -%}
INSERT INTO {{ t.state }}
SELECT asof, asset_id, max(slot) AS last, argMax(usd, slot), argMax(ppfs, slot) AS p, argMax(source, slot), argMax(want_asset_id, slot)
FROM (
  SELECT asset_id, slot,
    {%- if kind == 'oracle' %} value AS usd, nan AS ppfs,{% else %} usd, ppfs,{% endif %}
    source, want_asset_id
  FROM {{ t.slots if kind == 'oracle' else t.states }}
)
ARRAY JOIN arrayConcat(
  arrayMap(i -> toStartOfDay({{ lo }}) + toIntervalDay(i + 1), range(toUInt32(dateDiff('day', toStartOfDay({{ lo }}), toStartOfDay({{ hi }} - INTERVAL 1 SECOND))))),
  [{{ hi }}]
) AS asof
WHERE slot < asof
GROUP BY asof, asset_id
HAVING {% if kind == 'oracle' %}last >= asof - INTERVAL 1 DAY{% else %}NOT isNaN(p) OR last >= asof - INTERVAL 1 DAY{% endif %}
{{ price_resolver_settings() }}
{%- endmacro %}

{# Dirty assets of a tick: series with rows written since the last run (min ts per series), series whose map row or
   corrections changed (from their first value), then the vaults holding a dirty want. Per asset: the day its recompute
   starts and the slot of its first changed value (prices before that slot are unchanged). #}
{% macro price_dirty_assets(t, obs_from, obs_to, watermark) -%}
INSERT INTO {{ t.assets }}
WITH
  new_obs AS (
    SELECT series_id, min(ts) AS from_ts FROM {{ t.obs }}
    WHERE ingested_at > {{ obs_from }} AND ingested_at <= {{ obs_to }}
    GROUP BY series_id
  ),
  -- nan-safe compare: ppfs_log10 is nan on most rows.
  map_cols AS (SELECT *, toString(ppfs_log10) AS pl FROM {{ t.map }}),
  applied_cols AS (SELECT *, toString(ppfs_log10) AS pl FROM {{ t.map_applied }}),
  map_changed AS (
    SELECT series_id, asset_id FROM map_cols
    WHERE (series_id, source, asset_id, asset_kind, obs_kind, priority, want_asset_id, pl, scale, scale_until, valid_from, valid_to) NOT IN (SELECT series_id, source, asset_id, asset_kind, obs_kind, priority, want_asset_id, pl, scale, scale_until, valid_from, valid_to FROM applied_cols)
    UNION ALL
    SELECT series_id, asset_id FROM applied_cols
    WHERE (series_id, source, asset_id, asset_kind, obs_kind, priority, want_asset_id, pl, scale, scale_until, valid_from, valid_to) NOT IN (SELECT series_id, source, asset_id, asset_kind, obs_kind, priority, want_asset_id, pl, scale, scale_until, valid_from, valid_to FROM map_cols)
  ),
  corr AS ({{ price_corrections_select(t) }}),
  corr_changed AS (
    SELECT series_id, min(ts) AS from_ts FROM (
      SELECT series_id, ts FROM corr WHERE (series_id, ts, value) NOT IN (SELECT series_id, ts, value FROM {{ t.corr_applied }})
      UNION ALL
      SELECT series_id, ts FROM {{ t.corr_applied }} WHERE (series_id, ts, value) NOT IN (SELECT series_id, ts, value FROM corr)
    )
    GROUP BY series_id
  ),
  first_obs AS (
    SELECT series_id, min(ts) AS from_ts FROM (
      SELECT series_id, ts FROM {{ t.obs }} WHERE series_id IN (SELECT series_id FROM map_changed)
      UNION ALL
      SELECT series_id, ts FROM corr WHERE series_id IN (SELECT series_id FROM map_changed)
    )
    GROUP BY series_id
  ),
  direct AS (
    SELECT asset_id, min(from_ts) AS from_ts FROM (
      SELECT m.asset_id AS asset_id, greatest(d.from_ts, m.valid_from) AS from_ts
      FROM (SELECT * FROM new_obs UNION ALL SELECT * FROM corr_changed) d
      INNER JOIN {{ t.map }} m ON d.series_id = m.series_id
      WHERE d.from_ts < m.valid_to
      UNION ALL
      SELECT c.asset_id, f.from_ts FROM map_changed c INNER JOIN first_obs f ON c.series_id = f.series_id
    )
    GROUP BY asset_id
  ),
  -- A want price change reprices the vaults that hold the want.
  fanout AS (
    SELECT m.asset_id AS asset_id, min(x.from_ts) AS from_ts
    FROM direct x
    INNER JOIN (SELECT DISTINCT asset_id, want_asset_id FROM {{ t.map }} WHERE want_asset_id != 0) m
      ON m.want_asset_id = x.asset_id
    GROUP BY m.asset_id
  )
SELECT asset_id, toStartOfDay(min(from_ts)), toStartOfFiveMinutes(min(from_ts)) FROM (SELECT * FROM direct UNION ALL SELECT * FROM fanout)
GROUP BY asset_id
HAVING min(from_ts) < {{ watermark }}
{%- endmacro %}
