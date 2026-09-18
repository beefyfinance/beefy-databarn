{% test keys_match(model, to, columns, to_columns=None, to_where=None) %}

{#
  Every selected tuple on this model must exist on `to` (this ⊆ to).
  Attach once on each relation for two-way consistency.

  ClickHouse-safe: EXCEPT DISTINCT, not LEFT JOIN ... IS NULL (join_use_nulls=0).
  EXCEPT without DISTINCT is ALL in ClickHouse and would fan out fact-table rows.
#}

{%- set to_columns = to_columns if to_columns is not none else columns -%}

{%- if to_columns | length != columns | length -%}
  {{ exceptions.raise_compiler_error(
    "keys_match: `columns` and `to_columns` must be the same length"
  ) }}
{%- endif -%}

select {{ columns | join(', ') }}
from {{ model }}
except distinct
select {{ to_columns | join(', ') }}
from {{ to }}
{%- if to_where %}
where {{ to_where }}
{%- endif %}

{% endtest %}
