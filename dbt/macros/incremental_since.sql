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
