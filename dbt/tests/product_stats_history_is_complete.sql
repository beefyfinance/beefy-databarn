-- Fails when a product has a metric in a month of an hourly model but int_product_stats__unified_1h does not,
-- or has rows in a month of unified but not in fact_product_stats_1h: a reimport stopped halfway leaves such holes.
-- Whole months, so this stays cheap over the full history. fact_product_stats_1h starts at 2021-07-31.
-- Not the current month: its last hours can still be loading.

{% set sources = [
  ('int_product_stats__tvl_1h', 'tvl_usd'),
  ('int_product_stats__apy_1h', 'apy'),
  ('int_product_stats__share_price_1h', 'share_token_price_usd'),
] %}

with unified as (
  select chain_id, product_address, toStartOfMonth(date_hour) as month,
    count(tvl_usd) as tvl_usd, count(apy) as apy, count(share_token_price_usd) as share_token_price_usd
  from {{ ref('int_product_stats__unified_1h') }}
  where date_hour >= '2021-08-01'
    and date_hour < toStartOfMonth(now())
  group by chain_id, product_address, month
),

stats as (
  select distinct chain_id, product_address, toStartOfMonth(date_hour) as month
  from {{ ref('fact_product_stats_1h') }}
)

{% for name, column in sources %}
select '{{ name }}' as missing_from_next, s.chain_id as chain_id, s.product_address as product_address, s.month as month
from (
  select distinct chain_id, product_address, toStartOfMonth(date_hour) as month
  from {{ ref(name) }}
  where date_hour >= '2021-08-01'
    and date_hour < toStartOfMonth(now())
    and {{ column }} is not null
) s
left anti join (select * from unified where {{ column }} > 0) u
  on s.chain_id = u.chain_id
  and s.product_address = u.product_address
  and s.month = u.month

union all
{% endfor %}

select 'int_product_stats__unified_1h' as missing_from_next, u.chain_id as chain_id, u.product_address as product_address, u.month as month
from unified u
inner join {{ ref('dim_product') }} p
  on u.chain_id = p.chain_id
  and u.product_address = p.product_address
left anti join stats s
  on u.chain_id = s.chain_id
  and u.product_address = s.product_address
  and u.month = s.month
