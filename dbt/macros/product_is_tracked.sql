{#
  Product stats stop 3 months after a product retires or its chain goes eol (int_product_stats__tracking).
  The Beefy API keeps serving retired vaults with their last APY frozen, so without a cutoff they would show a
  metric forever. Rows already written are kept: this only stops new ones.

  Applied to the snapshot sources (tvl, apy, apy_breakdown, lps_breakdown, share_price). Not to harvests and yield:
  they are on-chain events (a retired vault can still be harvested), and those models are full rebuilds, where a
  cutoff would delete rows already written.

  Use as a HAVING (or WHERE) on the hourly product models:
    GROUP BY chain_id, product_address, date_hour
    HAVING {{ product_is_tracked() }}

  transform() looks the product up in a hash table built once from the constant arrays (a few thousand products
  with a cutoff), so it costs little per row. Products without a cutoff fall through to the default date.
#}
{% macro product_is_tracked(chain_id='chain_id', product_address='product_address', date_hour='date_hour') %}
  {%- set pairs = "arraySort(groupArray((concat(toString(chain_id), ':', product_address), assumeNotNull(tracked_until))))" -%}
  {%- set source = "FROM " ~ ref('int_product_stats__tracking') ~ " WHERE tracked_until IS NOT NULL" -%}
  {#- Keys and dates come from two subqueries: sorting the (key, date) pairs keeps them in the same order. -#}
  {{ date_hour }} < transform(
    concat(toString({{ chain_id }}), ':', toString({{ product_address }})),
    (SELECT arrayMap(x -> x.1, {{ pairs }}) {{ source }}),
    (SELECT arrayMap(x -> x.2, {{ pairs }}) {{ source }}),
    toDateTime('2106-01-01 00:00:00', 'UTC')
  )
{%- endmacro %}
