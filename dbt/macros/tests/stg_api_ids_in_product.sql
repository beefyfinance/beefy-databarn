{% test stg_api_ids_in_product(model, from_model, where_clause=None) %}

{#
  Beefy API vault ids must exist in product, including new vaults that have
  never harvested. analytics.harvests inner-joins on beefy_key, so a missing
  product row drops every future harvest for that vault.

  ClickHouse-safe: NOT IN, not LEFT JOIN ... IS NULL (join_use_nulls=0).
#}

select
  v.id as beefy_key,
  v.network,
  v.earn_contract_address,
  v.status
from {{ from_model }} v
where
  {% if where_clause %}
    {{ where_clause }}
  {% else %}
    1 = 1
  {% endif %}
  and v.id not in (
    select beefy_key from {{ model }}
  )

{% endtest %}
