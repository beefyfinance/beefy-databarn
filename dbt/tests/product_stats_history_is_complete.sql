-- Fails when a product has a metric in a month of an hourly model but int_product_stats__unified_1h does not,
-- or has the metric in a month of unified but not in fact_product_stats_1h: a reimport stopped halfway, or a
-- source that reached unified after the fact read those hours (apy was missing from the fact for years), leaves
-- such holes. Whole months, so this stays cheap over the full history. fact_product_stats_1h starts at 2021-07-31.
-- Not the current month: its last hours can still be loading.

{#- (hourly model, its column, the unified column, the fact column) -#}
{% set sources = [
  ('int_product_stats__tvl_1h', 'tvl_usd', 'tvl_usd', 'tvl_usd'),
  ('int_product_stats__apy_1h', 'apy', 'apy', 'apy'),
  ('int_product_stats__apy_breakdown_1h', 'total_apy', 'total_apy', 'apy'),
  ('int_product_stats__share_price_1h', 'share_token_price_usd', 'share_token_price_usd', 'share_price_usd'),
  ('int_product_stats__want_price_1h', 'want_price_usd', 'want_price_usd', 'want_price_usd'),
] %}

with unified as (
  select chain_id, product_address, toStartOfMonth(date_hour) as month,
    {% for _, _, u, _ in sources %}count({{ u }}) as {{ u }}_count, countIf({{ u }} > 0) as {{ u }}_positive{% if not loop.last %}, {% endif %}{% endfor %}
  from {{ ref('int_product_stats__unified_1h') }}
  where date_hour >= '2021-08-01'
    and date_hour < toStartOfMonth(now())
  group by chain_id, product_address, month
),

stats as (
  select chain_id, product_address, toStartOfMonth(date_hour) as month,
    count(tvl_usd) as tvl_usd, count(apy) as apy, count(share_price_usd) as share_price_usd, count(want_price_usd) as want_price_usd
  from {{ ref('fact_product_stats_1h') }}
  where date_hour < toStartOfMonth(now())
  group by chain_id, product_address, month
)

{% for name, column, u, _ in sources %}
select '{{ name }}' as missing_from_next, s.chain_id as chain_id, s.product_address as product_address, s.month as month
from (
  select distinct chain_id, product_address, toStartOfMonth(date_hour) as month
  from {{ ref(name) }}
  where date_hour >= '2021-08-01'
    and date_hour < toStartOfMonth(now())
    and {{ column }} is not null
) s
left anti join (select * from unified where {{ u }}_count > 0) u
  on s.chain_id = u.chain_id
  and s.product_address = u.product_address
  and s.month = u.month

union all
{% endfor %}

{#- Each fact column once (apy has two sources). Positive values on both sides: a want or share can be worth 0. -#}
{% set fact_columns = [] %}
{% set seen = [] %}
{% for _, _, u, f in sources %}
  {% if f not in seen %}{% do seen.append(f) %}{% do fact_columns.append((u, f)) %}{% endif %}
{% endfor %}
{% for u, f in fact_columns %}
select 'fact_product_stats_1h.{{ f }}' as missing_from_next, u.chain_id as chain_id, u.product_address as product_address, u.month as month
from unified u
inner join {{ ref('dim_product') }} p
  on u.chain_id = p.chain_id
  and u.product_address = p.product_address
left anti join (select * from stats where {{ f }} > 0) s
  on u.chain_id = s.chain_id
  and u.product_address = s.product_address
  and u.month = s.month
where u.{{ u }}_positive > 0
{% if not loop.last %}union all{% endif %}
{% endfor %}
