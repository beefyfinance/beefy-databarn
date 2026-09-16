{#
  ClickHouse overrides for dbt_utils generic tests.

  unique_combination_of_columns: count() (not count(*)), LIMIT on failures.
  accepted_range: project only the tested column and cap failure rows so a
  wide view (JSON/array conversions) is not fully materialized.

  Enable via dbt_project.yml dispatch search_order for dbt_utils.
#}

{% macro clickhouse__test_unique_combination_of_columns(model, combination_of_columns, quote_columns=false) %}

{% if not quote_columns %}
    {%- set column_list = combination_of_columns %}
{% elif quote_columns %}
    {%- set column_list = [] %}
    {% for column in combination_of_columns %}
        {% do column_list.append(adapter.quote(column)) %}
    {% endfor %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "`quote_columns` argument for unique_combination_of_columns test must be one of [True, False]. Got: '" ~ quote_columns ~ "'."
    ) }}
{% endif %}

{%- set columns_csv = column_list | join(', ') %}

select {{ columns_csv }}
from {{ model }}
group by {{ columns_csv }}
having count() > 1
limit 100

{% endmacro %}


{% macro clickhouse__test_accepted_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}

select {{ column_name }}
from {{ model }}
where
    1 = 0
    {%- if min_value is not none %}
    or not {{ column_name }} >{{- '=' if inclusive }} {{ min_value }}
    {%- endif %}
    {%- if max_value is not none %}
    or not {{ column_name }} <{{- '=' if inclusive }} {{ max_value }}
    {%- endif %}
limit 100

{% endmacro %}
