{#
  Cast an expression to the contract type (yml data_type) of a column of the current model, so the SELECT matches
  the enforced contract and a stored value compares equal to its recomputation (fact_product_stats_1h diffs its
  rows against unified this way). Returns the expression unchanged when the column has no data_type.

    {{ contract_cast('tvl_usd', 'hs.tvl_usd') }}  ->  CAST(hs.tvl_usd AS Nullable(Float64))
#}
{% macro contract_cast(column_name, expr) -%}
  {%- set columns = model.get('columns', {}) if model is mapping else {} -%}
  {%- set data_type = (columns.get(column_name) or {}).get('data_type') -%}
  {%- if data_type -%}CAST({{ expr }} AS {{ data_type }}){%- else -%}{{ expr }}{%- endif -%}
{%- endmacro %}
