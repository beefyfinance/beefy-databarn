-- Fails when a product has a metric in a month of an hourly model but int_product_stats__unified_hourly does not,
-- or has rows in a month of unified but not in product_stats: a reimport stopped halfway leaves such holes.
-- Whole months, so this stays cheap over the full history. product_stats starts at 2021-07-31.
-- Not the current month: its last hours can still be loading.

{% set sources = [
  ('int_product_stats__tvl_hourly', 'tvl_usd'),
  ('int_product_stats__apy_hourly', 'apy'),
  ('int_product_stats__share_price_hourly', 'share_token_price_usd'),
] %}

with unified as (
  select chain_id, product_address, toStartOfMonth(date_hour) as month,
    count(tvl_usd) as tvl_usd, count(apy) as apy, count(share_token_price_usd) as share_token_price_usd
  from {{ ref('int_product_stats__unified_hourly') }}
  where date_hour >= '2021-08-01'
    and date_hour < toStartOfMonth(now())
  group by chain_id, product_address, month
),

stats as (
  select distinct chain_id, product_address, toStartOfMonth(date_hour) as month
  from {{ ref('product_stats') }}
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

select 'int_product_stats__unified_hourly' as missing_from_next, u.chain_id as chain_id, u.product_address as product_address, u.month as month
from unified u
inner join {{ ref('product') }} p
  on u.chain_id = p.chain_id
  and u.product_address = p.product_address
left anti join stats s
  on u.chain_id = s.chain_id
  and u.product_address = s.product_address
  and u.month = s.month
