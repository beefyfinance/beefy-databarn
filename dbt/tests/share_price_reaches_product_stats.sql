-- Fails when a recent hourly share price is missing from product_stats or differs from the last 5-minute price of
-- that hour. Covers int_product_stats__share_price_hourly and int_product_stats__unified_hourly on the way.
-- The window ends 1 hour before the latest price: that hour can still be filling. It is anchored on the latest
-- price, not now(), so stale data does not empty it.

{% set window_days = var('share_price_propagation_days', 7) %}

with anchor as (
  select max(date_time) - interval 1 hour as window_end
  from {{ ref('int_product_share_price') }}
),

shares as (
  select
    chain_id,
    product_address,
    toStartOfHour(date_time) as date_hour,
    toFloat64(argMax(share_token_price_usd, date_time)) as share_price
  from {{ ref('int_product_share_price') }} FINAL
  where date_time >= (select window_end from anchor) - interval {{ window_days }} day
    and date_time < (select toStartOfHour(window_end) from anchor)
  group by chain_id, product_address, date_hour
),

stats as (
  select
    chain_id,
    product_address,
    date_hour,
    cast(toFloat64(share_token_price_usd) as Nullable(Float64)) as stats_price
  from {{ ref('product_stats') }}
  where date_hour >= (select window_end from anchor) - interval ({{ window_days }} + 1) day
)

select
  s.chain_id as chain_id,
  s.product_address as product_address,
  s.date_hour as date_hour,
  s.share_price as share_price,
  st.stats_price as stats_price
from shares s
left join stats st
  on s.chain_id = st.chain_id
  and s.product_address = st.product_address
  and s.date_hour = st.date_hour
where st.stats_price is null
  or abs(st.stats_price - s.share_price) > 1e-6 * abs(s.share_price)
