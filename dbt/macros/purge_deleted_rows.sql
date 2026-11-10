{#
  Physically drop the rows that delete+insert hid with lightweight deletes.

  A lightweight delete only masks rows (`_row_exists = 0`); they stay on disk, and every read still scans their mask,
  until a merge rewrites their part. A reimport masks a model's whole history, and the big old parts of an
  unpartitioned table hardly ever merge. Seen on prod 2026-11-10: fact_product_stats_1h held 457M rows for 178M
  visible (9 GiB for ~3), and `SELECT * ... LIMIT 20` read the mask of 173M rows (38 s instead of 0.4 s).

  Two remedies, both applied by the project-wide post-hook (dbt_project.yml) to incremental tables:
  - Merge settings: merges pick parts by their live size, so a mostly-deleted part merges soon and loses its masked
    rows. Set by ALTER, not by the `settings` config: dbt-clickhouse also appends that config to CREATE VIEW.
  - `ALTER TABLE ... APPLY DELETED MASK` after each run. It rewrites every part holding a masked row (all its
    columns): its cost is the size of those parts, not the number of deleted rows. The scheduler alerts on slow
    dbt runs (infra/dbt/alerts.py), where a costly purge would show. Skipped during a reimport (incremental_since):
    each monthly batch would rewrite the same big parts again; run_dbt_batched.py calls purge_deleted_rows once at
    the end instead:
      dbt run-operation purge_deleted_rows --args '{"models": ["fact_product_stats_1h"]}'
#}

{% macro purge_relation_deleted_rows(relation, set_merge_settings=false) %}
  {% if execute and relation is not none and not relation.is_view %}
    {% set state_sql %}
      SELECT
        position(engine_full, 'exclude_deleted_rows_for_part_size_in_merge') > 0 AS has_settings,
        (SELECT count() FROM system.parts
         WHERE active AND has_lightweight_delete AND database = '{{ relation.schema }}' AND table = '{{ relation.identifier }}') AS masked
      FROM system.tables WHERE database = '{{ relation.schema }}' AND name = '{{ relation.identifier }}'
    {% endset %}
    {% set state = run_query(state_sql) %}
    {% if state.rows | length > 0 %}
      {% if set_merge_settings and not state.rows[0][0] %}
        {{ log("purge_deleted_rows: merge settings on " ~ relation, info=True) }}
        {% do run_query("ALTER TABLE " ~ relation ~ " MODIFY SETTING exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1") %}
      {% endif %}
      {% if state.rows[0][1] > 0 %}
        {{ log("purge_deleted_rows: " ~ relation ~ " (" ~ state.rows[0][1] ~ " masked parts)", info=True) }}
        {% do run_query("ALTER TABLE " ~ relation ~ " APPLY DELETED MASK SETTINGS mutations_sync = 2") %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro purge_deleted_rows_hook() %}
  {% if execute and model.config.materialized in ['incremental', 'price_resolver'] and incremental_since_var() is none %}
    {% do purge_relation_deleted_rows(adapter.get_relation(database=none, schema=this.schema, identifier=this.identifier), set_merge_settings=true) %}
  {% endif %}
{% endmacro %}

{% macro purge_deleted_rows(models) %}
  {% for model_name in models %}
    {% do purge_relation_deleted_rows(adapter.get_relation(database=none, schema=ref(model_name).schema, identifier=ref(model_name).identifier), set_merge_settings=true) %}
  {% endfor %}
{% endmacro %}
