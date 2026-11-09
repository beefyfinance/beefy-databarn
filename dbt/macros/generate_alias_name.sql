{#-
  Table name of a model from its layer (CONVENTIONS.md). The first folder under models/ is the layer:
  - every model name starts with its layer's prefix;
  - the table is the model name (databarn.fact_product_stats_1h, dbt.int_tvl), except in api and legacy, which have a
    database of their own and drop the prefix (api_revenue_summary -> api.revenue_summary,
    legacy_product_stats -> analytics.product_stats);
  - facts and marts end with their grain (_obs, _5m, _1h, _1d, _1w, _1mo, _latest).
  A model with its own alias keeps it.
-#}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- if custom_alias_name -%}
    {{ custom_alias_name | trim }}
  {%- elif node.resource_type != 'model' or node.package_name != project_name or node.fqn | length < 3 -%}
    {{ node.name }}
  {%- else -%}
    {%- set layer = node.fqn[1] -%}
    {%- set prefixes = {'staging': 'stg_', 'intermediate': 'int_', 'data_quality': 'dq_',
                        'dimensions': 'dim_', 'facts': 'fact_', 'marts': 'mart_', 'api': 'api_',
                        'legacy': 'legacy_'} -%}
    {%- set stripped = ['api', 'legacy'] -%}
    {%- set grains = ['_obs', '_5m', '_1h', '_1d', '_1w', '_1mo', '_latest'] -%}
    {%- if layer not in prefixes -%}
      {{ exceptions.raise_compiler_error("Model " ~ node.name ~ " is in models/" ~ layer ~ "/, which is not a layer: " ~ (prefixes.keys() | join(', '))) }}
    {%- endif -%}
    {%- set prefix = prefixes[layer] -%}
    {%- if not node.name.startswith(prefix) -%}
      {{ exceptions.raise_compiler_error("Model " ~ node.name ~ " in models/" ~ layer ~ "/ must be named " ~ prefix ~ "<name>") }}
    {%- endif -%}
    {%- if layer in ['facts', 'marts'] -%}
      {%- set ns = namespace(ok=false) -%}
      {%- for g in grains -%}{%- if node.name.endswith(g) -%}{%- set ns.ok = true -%}{%- endif -%}{%- endfor -%}
      {%- if not ns.ok -%}
        {{ exceptions.raise_compiler_error("Model " ~ node.name ~ " in models/" ~ layer ~ "/ must end with its grain: " ~ (grains | join(', '))) }}
      {%- endif -%}
    {%- endif -%}
    {{ node.name[prefix | length:] if layer in stripped else node.name }}
  {%- endif -%}
{%- endmacro %}
