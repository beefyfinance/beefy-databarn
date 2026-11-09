{#- Pre-hook of fact_account_balance_1d: delete the days its incremental run rebuilds (from the day before its last
    day, or the incremental_since / incremental_until range), so rows no longer produced (a position closed by a late
    change) go away. Runs after the model is compiled, on the same table state, so the window matches the model's. -#}
{% macro account_balance_daily_clear_window() -%}
  {%- if is_incremental() -%}
    {%- set since = incremental_since_var() -%}
    {%- set until = incremental_until_var() -%}
    DELETE FROM {{ this }}
    WHERE date_day >= {% if since is not none %}toDate('{{ since }}'){% else %}(SELECT max(date_day) - 1 FROM {{ this }}){% endif %}
    {%- if until is not none %} AND date_day < toDate('{{ until }}'){% endif %}
  {%- else -%}
    SELECT 1
  {%- endif -%}
{%- endmacro %}
