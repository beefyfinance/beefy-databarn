{#
  Override dbt-clickhouse primary_key_clause.

  dbt-core 1.12+ warns when config.get('primary_key') misses adapter config but
  finds the same key under config.meta. This project stores logical uniqueness
  in meta.primary_key (docs / tests). ClickHouse sort/primary key is order_by;
  a meta PK is often a different column order and is not a valid MergeTree
  PRIMARY KEY (must be a prefix of ORDER BY).

  Only emit PRIMARY KEY when the user set adapter config (SQL config() or YAML
  config.primary_key), never when the value lives only in meta.
#}

{% macro primary_key_clause(label) %}
  {%- set primary_key = none -%}
  {#- Never config.get('primary_key'): dbt-core 1.12+ warns when that key
      lives only under meta, even if unrendered_config also surfaces it. -#}
  {%- set unrendered_pk = config.model.unrendered_config.get('primary_key') -%}
  {%- set meta_pk = config.meta_get('primary_key') -%}
  {%- if unrendered_pk is not none and unrendered_pk != meta_pk -%}
    {%- set primary_key = unrendered_pk -%}
  {%- elif unrendered_pk is not none and meta_pk is none -%}
    {%- set primary_key = unrendered_pk -%}
  {%- endif -%}
  {#- An empty value ('' or []) would emit "PRIMARY KEY" with no columns,
      which is invalid DDL - treat it as unset instead. -#}
  {%- if primary_key is not none and primary_key | length == 0 -%}
    {%- set primary_key = none -%}
  {%- endif -%}
  {#- v2 compatibility: v2's typed primary_key config arrives as a list
      (scalar inputs are listified). Render it as a single parenthesized
      expression, since bare "PRIMARY KEY a, b" is a ClickHouse syntax error.
      Guarded like the incremental macros' unique_key handling. -#}
  {%- if primary_key is not none and primary_key is iterable and (primary_key is not string and primary_key is not mapping) -%}
    {%- set primary_key = '(' ~ (primary_key | join(', ')) ~ ')' -%}
  {%- endif %}

  {%- if primary_key is not none %}
    {{ label }} {{ primary_key }}
  {%- endif %}
{%- endmacro -%}
