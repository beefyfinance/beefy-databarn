{#
  Materialization of fact_asset_price_5m: the dense 5-minute price of every asset, resolved from int_price_obs with
  int_price_series_map (SQL in macros/prices/price_resolver_sql.sql).

  - First run, --full-refresh: full build, all oracle assets first (a vault's ppfs takes its want price), then shares.
  - --vars incremental_since: rebuild every asset from that day on.
  - Otherwise a tick:
      1. dirty assets: series with rows written to int_price_obs since the last run (from their min ts), series
         whose map row or correction rows changed (from their first value), and the vaults holding a dirty want;
      2. recompute each dirty asset from the start of the day of its earliest change (one delete, masks applied);
      3. resolve the new slots of every asset, up to now minus var price_settle_minutes (rows of a slot have that
         long to arrive before the slot is resolved; later ones make it dirty);
      4. backfill: carry first values back to the creation of their vault or CLM (price_backfill).
  Each run appends a row to int_price_runs (head range, dirty assets with their start day and first changed slot):
  fact_asset_price_1d and the priced models read it to recompute what changed (macros/prices/price_runs.sql).
  Carry state: the last value of each asset at the end of every resolved range and at every day start, so a range
  starts from it instead of re-reading raw values back in time.
#}

{% macro price_ts(dt) -%}
  toDateTime('{{ dt.strftime("%Y-%m-%d %H:%M:%S") }}', 'UTC')
{%- endmacro %}

{# A ClickHouse Date or DateTime string as a datetime. #}
{% macro price_parse(s) -%}
  {%- set x = (s | string)[:19] -%}
  {{ return(modules.datetime.datetime.strptime(x if (x | length) > 10 else x ~ ' 00:00:00', '%Y-%m-%d %H:%M:%S')) }}
{%- endmacro %}

{% macro price_q(sql, label='') %}
  {%- set t0 = modules.datetime.datetime.now() -%}
  {%- set res = run_query(sql) -%}
  {%- if label -%}
    {{ log('  price ' ~ label ~ ': ' ~ ((modules.datetime.datetime.now() - t0).total_seconds() | round(1)) ~ 's', info=True) }}
  {%- endif -%}
  {{ return(res) }}
{% endmacro %}

{% macro price_scalar(sql) %}
  {{ return(run_query(sql).columns[0][0]) }}
{% endmacro %}

{# Month starts covering [lo, hi), by index (a variable set in a Jinja loop does not survive the iteration). #}
{% macro price_month_starts(lo, hi) %}
  {%- set out = [] -%}
  {%- set n = (hi.year - lo.year) * 12 + hi.month - lo.month + 1 -%}
  {%- for i in range(n) -%}
    {%- set k = lo.month - 1 + i -%}
    {%- do out.append(modules.datetime.datetime(lo.year + k // 12, k % 12 + 1, 1)) -%}
  {%- endfor -%}
  {{ return(out) }}
{% endmacro %}

{# Months of [lo, hi): a full build resolves one month after another (ranges inside). #}
{% macro price_months(lo, hi) %}
  {%- set starts = price_month_starts(lo, hi + modules.datetime.timedelta(days=31)) -%}
  {%- set out = [] -%}
  {%- for i in range(starts | length - 1) -%}
    {%- set m_lo, m_hi = [starts[i], lo] | max, [starts[i + 1], hi] | min -%}
    {%- if m_lo < m_hi -%}{%- do out.append((m_lo, m_hi)) -%}{%- endif -%}
  {%- endfor -%}
  {{ return(out) }}
{% endmacro %}

{# Resolver ranges of [lo, hi): cut at month starts, every var price_resolver_range_days (7) inside a month, and at
   each extra cut (dirty start days). #}
{% macro price_ranges(lo, hi, cuts=[], step_days=none) %}
  {%- set marks = [lo, hi] -%}
  {%- for m in price_month_starts(lo, hi) -%}
    {%- set step = step_days or (var('price_resolver_range_days', 7) | int) -%}
    {%- for w in range(31 // step + 1) -%}
      {%- set x = m + modules.datetime.timedelta(days=step * w) -%}
      {%- if x.month == m.month and lo < x < hi -%}{%- do marks.append(x) -%}{%- endif -%}
    {%- endfor -%}
  {%- endfor -%}
  {%- for c in cuts -%}{%- if lo < c < hi -%}{%- do marks.append(c) -%}{%- endif -%}{%- endfor -%}
  {%- set marks = marks | unique | sort -%}
  {%- set out = [] -%}
  {%- for i in range(marks | length - 1) -%}{%- do out.append((marks[i], marks[i + 1])) -%}{%- endfor -%}
  {{ return(out) }}
{% endmacro %}

{# Write the slots [lo, hi) of the assets of one kind passing flt. Memory of a step grows with assets x slots of its
   range: var price_resolver_chunks splits ranges longer than 2 days by asset (shares grouped by want, so each chunk's
   want-price join holds a fraction of the wants). On a server with about 9 GiB, one chunk per week ran out of memory
   on 2022 weeks; 4 chunks peak at about 0.7 GiB. #}
{% macro price_resolve(t, kind, lo, hi, flt='1', chunks=none) %}
  {%- set chunks = chunks or ((var('price_resolver_chunks', 4) | int) if (hi - lo).total_seconds() > 2 * 86400 else 1) -%}
  {%- for k in range(chunks) -%}
    {%- if chunks == 1 -%}
      {%- set f = flt -%}
    {%- elif kind == 'share' -%}
      {%- set f = '(' ~ flt ~ ') AND asset_id IN (SELECT asset_id FROM ' ~ t.map ~ " WHERE asset_kind = 'share' AND want_asset_id % " ~ chunks ~ ' = ' ~ k ~ ')' -%}
    {%- else -%}
      {%- set f = '(' ~ flt ~ ') AND asset_id % ' ~ chunks ~ ' = ' ~ k -%}
    {%- endif -%}
    {%- set l, h = price_ts(lo), price_ts(hi) -%}
    {%- set label = kind ~ ' ' ~ lo.strftime('%Y-%m-%d %H:%M') ~ '..' ~ hi.strftime('%m-%d %H:%M') ~ (' [' ~ (k + 1) ~ '/' ~ chunks ~ ']' if chunks > 1 else '') -%}
    {%- do price_q('TRUNCATE TABLE ' ~ t.slots) -%}
    {%- do price_q(price_stage_slots(t, kind, l, h, f)) -%}
    {%- if kind == 'oracle' -%}
      {%- do price_q(price_carry_in(t, 'oracle', l, f)) -%}
      {%- do price_q(price_stage1_write(t, l, h)) -%}
      {%- do price_q(price_bounds_out(t, l)) -%}
    {%- else -%}
      {%- do price_q('TRUNCATE TABLE ' ~ t.states) -%}
      {%- do price_q(price_stage2_states(t, l, h)) -%}
      {%- do price_q(price_carry_in(t, 'share', l, f)) -%}
      {%- do price_q(price_stage2_write(t, l, h)) -%}
    {%- endif -%}
    {%- do price_q(price_state_out(t, kind, l, h), label) -%}
  {%- endfor -%}
{% endmacro %}

{# Backfill step of a run (SQL and rules in macros/prices/price_resolver_sql.sql): after the slots are resolved, carry
   first values back to creation for the candidates, rewrite those that changed or were recomputed, and add the changed
   ones to the run's dirty assets (t.assets) so the priced models reprice them. run_kind 'full' writes into a fresh
   table (nothing to delete, nothing to report); 'rebuild' takes every asset. The first run with an empty
   int_price_backfills (the deploy of the backfill) reports nothing either: it would make the priced models reprice
   the whole history of every backfilled asset in one run. Their rows before the first prices stay unpriced until a
   batched reimport. #}
{% macro price_backfill(t, run_kind, watermark) %}
  {%- set everything = run_kind != 'tick' -%}
  {%- set bootstrap = run_kind == 'tick' and price_scalar('SELECT count() = 0 FROM ' ~ t.backfills) -%}
  {%- for x in [t.bf_cand, t.bf_first, t.bf_new, t.bf_rewrite] -%}{%- do price_q('TRUNCATE TABLE ' ~ x) -%}{%- endfor -%}
  {%- if run_kind == 'full' -%}{%- do price_q('TRUNCATE TABLE ' ~ t.backfills) -%}{%- endif -%}
  {%- if run_kind == 'rebuild' -%}{%- do price_q('TRUNCATE TABLE ' ~ t.assets) -%}{%- endif -%}
  {%- do price_q(price_backfill_candidates(t, everything, price_ts(watermark))) -%}
  {%- if price_scalar('SELECT count() FROM ' ~ t.bf_cand) > 0 -%}
    {%- do price_q(price_backfill_first(t)) -%}
    {%- do price_q(price_backfill_new(t)) -%}
    {%- do price_q(price_backfill_rewrite(t, everything)) -%}
    {%- set n = run_query('SELECT count(), countIf(changed) FROM ' ~ t.bf_rewrite).rows[0] -%}
    {%- if n[0] > 0 -%}
      {%- if run_kind != 'full' -%}
        {%- do price_q('DELETE FROM ' ~ t.target ~ ' WHERE asset_id IN (SELECT asset_id FROM ' ~ t.bf_rewrite ~ ') AND observed_at > date_time') -%}
      {%- endif -%}
      {%- if run_kind != 'full' and not bootstrap -%}
        {%- do price_q('INSERT INTO ' ~ t.assets ~ ' SELECT asset_id, toStartOfDay(from_ts), from_ts FROM ' ~ t.bf_rewrite ~ ' WHERE changed') -%}
      {%- endif -%}
      {%- do price_q(price_backfill_write(t, false)) -%}
      {%- do price_q(price_backfill_write(t, true)) -%}
    {%- endif -%}
    {%- do price_q('DELETE FROM ' ~ t.backfills ~ ' WHERE asset_id IN (SELECT asset_id FROM ' ~ t.bf_cand ~ ')') -%}
    {%- do price_q('INSERT INTO ' ~ t.backfills ~ ' SELECT * FROM ' ~ t.bf_new) -%}
    {{ log('  price backfill: ' ~ price_scalar('SELECT count() FROM ' ~ t.bf_cand) ~ ' candidates, ' ~ n[0] ~ ' rewritten, ' ~ n[1] ~ ' changed' ~ (' (first backfill: not reported to the priced models)' if bootstrap else ''), info=True) }}
  {%- endif -%}
{% endmacro %}

{# A lightweight delete only masks rows; applying the mask drops them from disk (about 1 s a month). #}
{% macro price_apply_masks(relation) %}
  {%- set rows = run_query("SELECT partition_id FROM system.parts WHERE database = '" ~ relation.schema ~ "' AND table = '"
      ~ relation.identifier ~ "' AND active AND has_lightweight_delete GROUP BY partition_id").rows -%}
  {%- for row in rows -%}{%- set pid = row[0] -%}
    {%- do run_query("ALTER TABLE " ~ relation ~ " APPLY DELETED MASK IN PARTITION ID '" ~ pid ~ "' SETTINGS mutations_sync = 2") -%}
  {%- endfor -%}
{% endmacro %}

{% macro price_snapshot_inputs(t) %}
  {%- do price_q('TRUNCATE TABLE ' ~ t.map_applied) -%}
  {%- do price_q('INSERT INTO ' ~ t.map_applied ~ ' SELECT * FROM ' ~ t.map) -%}
  {%- do price_q('TRUNCATE TABLE ' ~ t.corr_applied) -%}
  {%- do price_q('INSERT INTO ' ~ t.corr_applied ~ ' SELECT series_id, ts, value FROM (' ~ price_corrections_select(t) ~ ')') -%}
{% endmacro %}

{% materialization price_resolver, adapter='clickhouse' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set t = price_resolver_tables(target_relation) -%}
  {%- set td = modules.datetime.timedelta -%}
  {%- set existed = load_cached_relation(this) is not none -%}
  {{ run_hooks(pre_hooks) }}

  {%- for s in price_resolver_ddl(t) -%}{%- do run_query(s) -%}{%- endfor -%}

  {%- set settle = var('price_settle_minutes', 10) | int -%}
  {%- set now_end = price_parse(price_scalar("SELECT toString(toStartOfFiveMinutes(now() - INTERVAL " ~ settle ~ " MINUTE))")) -%}
  {%- set obs_to_s = price_scalar('SELECT toString(max(ingested_at)) FROM ' ~ t.obs) -%}
  {%- set last = run_query("SELECT toString(watermark), toString(obs_mark) FROM " ~ t.runs ~ " ORDER BY run_id DESC LIMIT 1") -%}
  {%- set since = incremental_since_var() -%}
  {#- A table of an older schema (no observed_at) is rebuilt: a tick could not write to it. -#}
  {%- set outdated = existed and price_scalar("SELECT count() = 0 FROM system.columns WHERE database = '" ~ target_relation.schema
      ~ "' AND table = '" ~ target_relation.identifier ~ "' AND name = 'observed_at'") -%}
  {%- if outdated -%}{{ log('price_resolver: ' ~ target_relation ~ ' has no observed_at column, full build', info=True) }}{%- endif -%}
  {%- set full = flags.FULL_REFRESH or not existed or outdated or (last.rows | length) == 0 -%}
  {%- set run_kind = 'full' if full else ('rebuild' if since is not none else 'tick') -%}
  {%- set dirty_sql = "[]" -%}
  {{ log('price_resolver: ' ~ run_kind ~ ' up to ' ~ now_end, info=True) }}

  {%- if run_kind == 'full' -%}
    {#- Built into a new table, swapped in at the end: readers keep the old table meanwhile, and a table of an older
        schema is replaced. -#}
    {%- set build = target_relation.incorporate(path={'identifier': target_relation.identifier ~ '__build'}) -%}
    {%- do price_q('DROP TABLE IF EXISTS ' ~ build) -%}
    {%- set tb = {} -%}{%- do tb.update(t) -%}{%- do tb.update({'target': build}) -%}
    {%- do price_q(price_resolver_ddl(tb)[0]) -%}
    {%- for x in [t.state, t.bounds, t.runs] -%}{%- do price_q('TRUNCATE TABLE ' ~ x) -%}{%- endfor -%}
    {%- set head_from = price_parse(price_scalar('SELECT toString(toStartOfMonth(min(ts))) FROM ' ~ t.obs)) -%}
    {%- set head_to = now_end -%}
    {%- for kind in ['oracle', 'share'] -%}
      {%- for m_lo, m_hi in price_months(head_from, head_to) -%}
        {%- for r_lo, r_hi in price_ranges(m_lo, m_hi) -%}{%- do price_resolve(tb, kind, r_lo, r_hi) -%}{%- endfor -%}
      {%- endfor -%}
    {%- endfor -%}
    {%- do price_backfill(tb, 'full', head_to) -%}
    {%- do price_q('EXCHANGE TABLES ' ~ target_relation ~ ' AND ' ~ build) -%}
    {%- do price_q('DROP TABLE ' ~ build) -%}

  {%- else -%}
    {%- set watermark = price_parse(last.rows[0][0]) -%}
    {%- set obs_from_s = last.rows[0][1] -%}

    {%- if run_kind == 'rebuild' -%}
      {%- set start = price_parse(since).replace(hour=0, minute=0, second=0) -%}
      {%- do price_q('DELETE FROM ' ~ t.target ~ ' WHERE date_time >= ' ~ price_ts(start), 'delete rebuild') -%}
      {#- Carry states after the start are rebuilt; an asset with no value left in a day writes none, so its old state
          would carry a stale value into that day. -#}
      {%- do price_q('DELETE FROM ' ~ t.state ~ ' WHERE asof > ' ~ price_ts(start), 'delete rebuild states') -%}
      {%- do price_apply_masks(target_relation) -%}
      {%- for kind in ['oracle', 'share'] -%}
        {%- for r_lo, r_hi in price_ranges(start, watermark) -%}{%- do price_resolve(t, kind, r_lo, r_hi) -%}{%- endfor -%}
      {%- endfor -%}
      {%- set head_from = start -%}

    {%- else -%}
      {%- do price_q('TRUNCATE TABLE ' ~ t.assets) -%}
      {%- do price_q(price_dirty_assets(t, "toDateTime64('" ~ obs_from_s ~ "', 3, 'UTC')", "toDateTime64('" ~ obs_to_s ~ "', 3, 'UTC')", price_ts(watermark)), 'dirty assets') -%}
      {%- set days = [] -%}
      {%- for row in run_query('SELECT toString(from_day) FROM ' ~ t.assets ~ ' GROUP BY from_day ORDER BY from_day').rows -%}{%- do days.append(row[0] | string) -%}{%- endfor -%}
      {%- if days | length > 0 -%}
        {%- set cuts = [] -%}{%- for d in days -%}{%- do cuts.append(price_parse(d)) -%}{%- endfor -%}
        {{ log('price_resolver: ' ~ price_scalar('SELECT count() FROM ' ~ t.assets) ~ ' dirty assets from ' ~ days[0], info=True) }}
        {%- do price_q('DELETE FROM ' ~ t.target ~ ' WHERE (asset_id, toStartOfDay(date_time)) IN (SELECT asset_id, '
            ~ "arrayJoin(arrayMap(i -> from_day + toIntervalDay(i), range(toUInt32(dateDiff('day', from_day, " ~ price_ts(watermark) ~ ')) + 1))) FROM ' ~ t.assets
            ~ ') AND date_time < ' ~ price_ts(watermark), 'delete dirty') -%}
        {#- Their carry states after from_day too (the state at from_day is the recompute's carry-in): the recompute
            writes a state only while the asset's last value is under a day old, so a value that no longer exists
            (a series unmapped or cut short) would leave its old state carrying into later days. -#}
        {%- do price_q('DELETE FROM ' ~ t.state ~ ' WHERE (asset_id, toStartOfDay(asof)) IN (SELECT asset_id, '
            ~ "arrayJoin(arrayMap(i -> from_day + toIntervalDay(i), range(toUInt32(dateDiff('day', from_day, " ~ price_ts(watermark) ~ ')) + 1))) FROM ' ~ t.assets
            ~ ') AND (asset_id, asof) NOT IN (SELECT asset_id, from_day FROM ' ~ t.assets ~ ')', 'delete dirty states') -%}
        {#- A few dirty assets leave few dead rows (merges drop them); many leave whole months of them. -#}
        {%- set n_dirty, n_all = run_query('SELECT (SELECT count() FROM ' ~ t.assets ~ '), (SELECT uniqExact(asset_id) FROM ' ~ t.map ~ ')').rows[0] -%}
        {%- if n_dirty * 10 >= n_all -%}{%- do price_apply_masks(target_relation) -%}{%- endif -%}
        {%- for kind in ['oracle', 'share'] -%}
          {#- Memory grows with the assets of a range: a few dirty assets (under a tenth of their kind) run in month
              ranges and one chunk; a kind with none is skipped. -#}
          {%- set counts = run_query("SELECT countIf(asset_id IN (SELECT asset_id FROM " ~ t.assets ~ ")), count() FROM (SELECT DISTINCT asset_id FROM "
              ~ t.map ~ " WHERE asset_kind = '" ~ kind ~ "')").rows[0] -%}
          {%- if counts[0] > 0 -%}
            {%- set few = counts[0] * 10 < counts[1] -%}
            {%- for r_lo, r_hi in price_ranges(cuts[0], watermark, cuts, 31 if few else none) -%}
              {%- do price_resolve(t, kind, r_lo, r_hi, 'asset_id IN (SELECT asset_id FROM ' ~ t.assets ~ ' WHERE from_day <= ' ~ price_ts(r_lo) ~ ')', 1 if few else none) -%}
            {%- endfor -%}
          {%- endif -%}
        {%- endfor -%}
        {%- set dirty_sql = '(SELECT groupArray((asset_id, from_day, from_ts)) FROM ' ~ t.assets ~ ')' -%}
      {%- endif -%}
      {%- set head_from = watermark -%}
    {%- endif -%}

    {%- set head_to = [now_end, watermark] | max -%}
    {%- for kind in ['oracle', 'share'] -%}
      {%- for r_lo, r_hi in price_ranges(watermark, head_to) -%}{%- do price_resolve(t, kind, r_lo, r_hi) -%}{%- endfor -%}
    {%- endfor -%}
    {%- do price_backfill(t, run_kind, watermark) -%}
    {%- if price_scalar('SELECT count() FROM ' ~ t.assets) > 0 -%}
      {%- set dirty_sql = '(SELECT groupArray((asset_id, from_day, from_ts)) FROM ' ~ t.assets ~ ')' -%}
    {%- endif -%}
  {%- endif -%}

  {%- do price_snapshot_inputs(t) -%}
  {% call statement('main') -%}
    INSERT INTO {{ t.runs }}
    SELECT toUnixTimestamp64Milli(now64(3)), now64(3), '{{ run_kind }}', {{ price_ts(head_to) }},
      toDateTime64('{{ obs_to_s }}', 3, 'UTC'), {{ price_ts(head_from) }}, {{ price_ts(head_to) }}, {{ dirty_sql }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
