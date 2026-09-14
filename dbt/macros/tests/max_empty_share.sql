{% test max_empty_share(model, column_name, max_share=0.1, nulls='empty', zeros='empty') %}

{#
  Fail when more than max_share of values are empty.
  Nulls and zeros are configured independently:
    empty     — counts as empty (default)
    ignore    — those rows are excluded from the share
    not_empty — not empty
#}

{%- if nulls not in ['empty', 'ignore', 'not_empty'] -%}
  {{ exceptions.raise_compiler_error("max_empty_share: nulls must be 'empty', 'ignore', or 'not_empty' (got " ~ nulls ~ ")") }}
{%- endif -%}
{%- if zeros not in ['empty', 'ignore', 'not_empty'] -%}
  {{ exceptions.raise_compiler_error("max_empty_share: zeros must be 'empty', 'ignore', or 'not_empty' (got " ~ zeros ~ ")") }}
{%- endif -%}

{%- set empty_parts = [] -%}
{%- if nulls == 'empty' -%}
  {%- do empty_parts.append(column_name ~ " is null") -%}
{%- endif -%}
{%- if zeros == 'empty' -%}
  {%- do empty_parts.append(column_name ~ " = 0") -%}
{%- endif -%}

{%- if empty_parts | length == 0 -%}
  {%- set empty_pred = "0" -%}
{%- else -%}
  {%- set empty_pred = "(" ~ empty_parts | join(" or ") ~ ")" -%}
{%- endif -%}

{%- set denom_filters = [] -%}
{%- if nulls == 'ignore' -%}
  {%- do denom_filters.append(column_name ~ " is not null") -%}
{%- endif -%}
{%- if zeros == 'ignore' -%}
  {%- do denom_filters.append("not (" ~ column_name ~ " = 0)") -%}
{%- endif -%}

{%- if denom_filters | length == 0 -%}
  {%- set denom = "count()" -%}
{%- else -%}
  {%- set denom = "countIf(" ~ denom_filters | join(" and ") ~ ")" -%}
{%- endif -%}

select
  countIf({{ empty_pred }}) / {{ denom }} as empty_share
from {{ model }}
having
  {{ denom }} = 0
  or countIf({{ empty_pred }}) / {{ denom }} > {{ max_share }}

{% endtest %}
