{#
  Optional rewind for incremental models.

  `make dbt reimport <model> YYYY-MM-DD` passes `--vars incremental_since`.
  When set, delete+insert starts at that timestamp instead of the usual lookback.
#}

{% macro incremental_since_var() %}
  {%- set since = var('incremental_since', none) -%}
  {%- if since is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set since_str = since | string | trim -%}
  {%- if since_str == '' or since_str | lower in ['none', 'null'] -%}
    {{ return(none) }}
  {%- endif -%}
  {{ return(since_str) }}
{% endmacro %}

{#
  Optional upper bound for a reimport window (exclusive), paired with incremental_since.
  Lets a large rewind run in batches: --vars '{"incremental_since": "2026-01-01", "incremental_until": "2026-02-01"}'.
  Only models that call it honour it.
#}

{% macro incremental_until_var() %}
  {%- set until = var('incremental_until', none) -%}
  {%- if until is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set until_str = until | string | trim -%}
  {%- if until_str == '' or until_str | lower in ['none', 'null'] -%}
    {{ return(none) }}
  {%- endif -%}
  {{ return(until_str) }}
{% endmacro %}
