{#
  beefy-history parquet on RustFS.

  ClickHouse reads current/ via named collections (infra/clickhouse/config.d/60-beefy-history-s3.xml).
  dbt copies those files into MergeTree tables — never query s3() from marts, never DuckDB.

  Schema surprises (CLI store):
  - `data` is canonical JSON text, not a nested parquet struct.
  - `type` on the event is added|changed|removed|readded. Config type lives in data.type.
  - `objects` / `latest` are DuckDB views, not files.
#}

{% macro beefy_history_s3_events() %}
    s3(beefy_history_s3_events)
{%- endmacro %}

{% macro beefy_history_s3_issues() %}
    s3(beefy_history_s3_issues)
{%- endmacro %}

{# Missing / JSON null / empty string → NULL (empty counts as active downstream). Non-strings as raw JSON text. #}
{% macro beefy_history_json_text(data_expr, key) %}
    if(
        {{ data_expr }} IS NULL
        OR JSONHas({{ data_expr }}, '{{ key }}') = 0
        OR JSONType({{ data_expr }}, '{{ key }}') = 'Null',
        NULL,
        if(
            JSONType({{ data_expr }}, '{{ key }}') = 'String',
            nullIf(JSONExtractString({{ data_expr }}, '{{ key }}'), ''),
            JSONExtractRaw({{ data_expr }}, '{{ key }}')
        )
    )
{%- endmacro %}

{% macro beefy_history_json_bool(data_expr, key) %}
    if(
        {{ data_expr }} IS NULL
        OR JSONHas({{ data_expr }}, '{{ key }}') = 0
        OR JSONType({{ data_expr }}, '{{ key }}') = 'Null',
        false,
        JSONExtract({{ data_expr }}, '{{ key }}', 'Bool')
    )
{%- endmacro %}

{# standard | gov | cowcentrated | erc4626. Fallback: isGovVault → gov, else standard. #}
{% macro beefy_history_vault_type(config_type_expr, is_gov_vault_expr) %}
    coalesce(
        nullIf({{ config_type_expr }}, ''),
        if({{ is_gov_vault_expr }}, 'gov', 'standard')
    )
{%- endmacro %}
