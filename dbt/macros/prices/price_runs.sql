{#
  Price consumers: a model that stores prices or values priced from fact_asset_price_5m recomputes what each resolver run
  changed. int_price_runs holds, per run, the head range it resolved and the dirty assets, each with the day its
  recompute started and the slot of its first changed price. A consumer reads the runs it has not applied yet
  (int_price_consumers keeps the last run each consumer applied, written by price_consumer_done as a post-hook).

  A consumer with no row yet has nothing pending: its first build (or a reimport) is its baseline.
#}

{% macro price_consumers_relation() -%}
  {{ ref('int_price_obs').schema }}.int_price_consumers
{%- endmacro %}

{% macro price_runs_relation() -%}
  {{ ref('int_price_obs').schema }}.int_price_runs
{%- endmacro %}

{# Key of a product in the repricing lists. #}
{% macro price_product_key(chain_id, product_address) -%}
  cityHash64(toInt64({{ chain_id }}), toString({{ product_address }}))
{%- endmacro %}

{% macro price_consumers_ddl() -%}
  CREATE TABLE IF NOT EXISTS {{ price_consumers_relation() }} (model String, run_id UInt64, updated_at DateTime64(3, 'UTC'))
  ENGINE = MergeTree ORDER BY (model, run_id)
{%- endmacro %}

{# Pending runs of this model: {'any': bool, 'last': run_id, 'head_from', 'head_to': SQL DateTime literals,
   'dirty': SQL select of (asset_id, from_day, from_ts), one row per asset}. #}
{% macro price_runs_pending(model_name) %}
  {%- set none_pending = {'any': false, 'last': 0, 'head_from': "toDateTime(0, 'UTC')", 'head_to': "toDateTime(0, 'UTC')", 'dirty': "SELECT toUInt64(0) AS asset_id, toDateTime(0, 'UTC') AS from_day, toDateTime(0, 'UTC') AS from_ts WHERE 0"} -%}
  {%- if not execute or adapter.get_relation(database=none, schema=ref('int_price_obs').schema, identifier='int_price_runs') is none -%}
    {{ return(none_pending) }}
  {%- endif -%}
  {%- do run_query(price_consumers_ddl()) -%}
  {%- set applied = run_query("SELECT count(), max(run_id) FROM " ~ price_consumers_relation() ~ " WHERE model = '" ~ model_name ~ "'").rows[0] -%}
  {%- if applied[0] == 0 -%}
    {{ return(none_pending) }}
  {%- endif -%}
  {%- set row = run_query("SELECT count(), max(run_id), toString(min(head_from)), toString(max(head_to)) FROM " ~ price_runs_relation() ~ " WHERE run_id > " ~ applied[1]).rows[0] -%}
  {{ return({
    'any': row[0] > 0,
    'last': row[1],
    'head_from': "toDateTime('" ~ row[2] ~ "', 'UTC')",
    'head_to': "toDateTime('" ~ row[3] ~ "', 'UTC')",
    'dirty': "SELECT d.asset_id AS asset_id, min(d.from_day) AS from_day, min(d.from_ts) AS from_ts FROM " ~ price_runs_relation() ~ " ARRAY JOIN dirty AS d WHERE run_id > " ~ applied[1] ~ " GROUP BY asset_id",
  }) }}
{% endmacro %}

{#
  What an incremental priced model must recompute beyond its own window (rows at or after `threshold`, a SQL-ready
  'YYYY-MM-DD hh:mm:ss' string): the rows priced by a dirty asset from its first changed slot, and, after a resolver
  catch-up, everything from the start of its head range. Changes at or after the threshold are already in the window,
  so routine late data adds nothing.
  grain: 'asset' (keys are asset ids) or 'product' (keys are price_product_key of the products priced by a dirty
  asset, through int_product_price_assets). unit: 'hour' or 'day' floors the start times, for hourly or daily
  models.
  Up to var price_reprice_max_keys keys (default 1000) the list is a literal, each key from its own start: an IN over
  a subquery of the run log is much slower, and a query is capped at 256 KiB. A longer list goes to the scratch table
  int_price_reprice_keys and each listed key is recomputed from the earliest start of the list.
  A head range starting more than var price_reprice_max_days (default 31) before the window is a re-resolve of
  fact_asset_price_5m (--full-refresh or incremental_since): recomputing it in one run does not fit in memory, so the
  model fails and names the batched reimport to run. A batched reimport (incremental_since) needs none of this and
  marks the runs applied.
  Use with price_repriced(rp, key_expr, time_expr).
#}
{% macro price_reprice(model_name, threshold, grain='asset', unit=none) %}
  {%- set rp = {'head_from': none, 'keys': [], 'times': [], 'scratch': none} -%}
  {%- set p = price_runs_pending(model_name) -%}
  {%- if not p['any'] or incremental_since_var() is not none or threshold is none -%}
    {{ return(rp) }}
  {%- endif -%}
  {%- set thr = "toDateTime('" ~ threshold ~ "', 'UTC')" -%}
  {%- set trunc_open = {'hour': 'toStartOfHour(', 'day': 'toStartOfDay('}.get(unit, '(') -%}
  {%- set head = run_query("SELECT " ~ p['head_from'] ~ " < " ~ thr ~ ", toString(" ~ trunc_open ~ p['head_from'] ~ ")), "
      ~ p['head_from'] ~ " < " ~ thr ~ " - INTERVAL " ~ (var('price_reprice_max_days', 31) | int) ~ " DAY").rows[0] -%}
  {%- if head[2] -%}
    {{ exceptions.raise_compiler_error(model_name ~ ": fact_asset_price_5m was re-resolved from " ~ head[1] ~ ", too far back to reprice in one run. "
        ~ "Run the batched reimport: make dbt reimport '" ~ model_name ~ "+' " ~ (head[1] | string)[:10]) }}
  {%- endif -%}
  {%- if head[0] -%}
    {%- do rp.update({'head_from': "toDateTime('" ~ head[1] ~ "', 'UTC')"}) -%}
    {#- Changes from the head range on are covered by it. -#}
    {%- set thr = "toDateTime('" ~ head[1] ~ "', 'UTC')" -%}
  {%- endif -%}
  {%- if grain == 'product' -%}
    {%- set sql -%}
      SELECT {{ price_product_key('pa.chain_id', 'pa.product_address') }} AS k, {{ trunc_open }}min(d.from_ts)) AS t
      FROM ({{ p['dirty'] }}) d
      INNER JOIN {{ ref('int_product_price_assets') }} pa ON pa.asset_id = d.asset_id
      WHERE d.from_ts < {{ thr }}
      GROUP BY k
    {%- endset -%}
  {%- else -%}
    {%- set sql -%}
      SELECT asset_id AS k, {{ trunc_open }}from_ts) AS t FROM ({{ p['dirty'] }}) WHERE from_ts < {{ thr }}
    {%- endset -%}
  {%- endif -%}
  {%- set n = run_query("SELECT count(), toString(min(t)) FROM (" ~ sql ~ ")").rows[0] -%}
  {%- if n[0] > var('price_reprice_max_keys', 1000) | int -%}
    {%- set scratch = ref('int_price_obs').schema ~ '.int_price_reprice_keys' -%}
    {%- do run_query("CREATE TABLE IF NOT EXISTS " ~ scratch ~ " (model String, key UInt64) ENGINE = MergeTree ORDER BY (model, key)") -%}
    {%- do run_query("DELETE FROM " ~ scratch ~ " WHERE model = '" ~ model_name ~ "'") -%}
    {%- do run_query("INSERT INTO " ~ scratch ~ " SELECT '" ~ model_name ~ "', k FROM (" ~ sql ~ ")") -%}
    {%- do rp.update({'scratch': "(SELECT key FROM " ~ scratch ~ " WHERE model = '" ~ model_name ~ "')", 'scratch_from': "toDateTime('" ~ n[1] ~ "', 'UTC')"}) -%}
  {%- elif n[0] > 0 -%}
    {%- for row in run_query("SELECT k, toString(t) FROM (" ~ sql ~ ") ORDER BY k").rows -%}
      {%- do rp['keys'].append(row[0] | string) -%}
      {%- do rp['times'].append("toDateTime('" ~ row[1] ~ "', 'UTC')") -%}
    {%- endfor -%}
  {%- endif -%}
  {%- if n[0] > 0 or rp['head_from'] is not none -%}
    {{ log(model_name ~ ': repricing ' ~ n[0] ~ ' ' ~ grain ~ 's' ~ ((' from ' ~ n[1] ~ ' (scratch list)') if rp['scratch'] else '') ~ ((' and everything from ' ~ head[1]) if rp['head_from'] else ''), info=True) }}
  {%- endif -%}
  {{ return(rp) }}
{% endmacro %}

{# Predicate: rows to reprice (see price_reprice); 0 when there are none. #}
{% macro price_repriced(rp, key_expr, time_expr) -%}
  {%- if rp['head_from'] is none and rp['keys'] | length == 0 and rp.get('scratch') is none -%}
    0
  {%- else -%}
    ({% if rp['head_from'] is not none %}{{ time_expr }} >= {{ rp['head_from'] }}{% else %}0{% endif %}
    {%- if rp['keys'] | length > 0 %}
      OR ({{ key_expr }} IN ({{ rp['keys'] | join(', ') }})
        AND {{ time_expr }} >= transform({{ key_expr }}, [{{ rp['keys'] | join(', ') }}], [{{ rp['times'] | join(', ') }}], toDateTime(4294967295, 'UTC')))
    {%- endif %}
    {%- if rp.get('scratch') is not none %}
      OR ({{ key_expr }} IN {{ rp['scratch'] }} AND {{ time_expr }} >= {{ rp['scratch_from'] }})
    {%- endif %})
  {%- endif -%}
{%- endmacro %}

{# Pre-hook of a daily rollup of fact_asset_price_5m (fact_asset_price_1d): delete the days its run rebuilds, the head range of
   the pending resolver runs and each dirty asset from its recompute day, so a day left with no 5-minute price (a
   series unmapped or cut short) loses its row. delete+insert alone only replaces the days the run produces. A
   reimport (incremental_since) rebuilds its own window and deletes nothing here. #}
{% macro price_rollup_clear_pending(model_name) -%}
  {%- set p = price_runs_pending(model_name) -%}
  {%- if is_incremental() and p['any'] and incremental_since_var() is none -%}
  DELETE FROM {{ this }}
  WHERE (date_day >= toDate({{ p['head_from'] }}) AND date_day <= toDate({{ p['head_to'] }} - INTERVAL 1 SECOND))
    OR (asset_id, date_day) IN (
      SELECT asset_id, toDate(arrayJoin(arrayMap(i -> from_day + toIntervalDay(i), range(toUInt32(dateDiff('day', from_day, {{ p['head_to'] }})) + 1))))
      FROM ({{ p['dirty'] }})
    )
  {%- else -%}
  SELECT 1
  {%- endif -%}
{%- endmacro %}

{# Post-hook: mark every resolver run up to now as applied by this model. #}
{% macro price_consumer_done(model_name) -%}
  {%- if not execute or adapter.get_relation(database=none, schema=ref('int_price_obs').schema, identifier='int_price_runs') is none -%}
  SELECT 1
  {%- else -%}
  {%- do run_query(price_consumers_ddl()) -%}
  INSERT INTO {{ price_consumers_relation() }}
  SELECT '{{ model_name }}', max(run_id), now64(3) FROM {{ price_runs_relation() }} HAVING count() > 0
  {%- endif -%}
{%- endmacro %}
