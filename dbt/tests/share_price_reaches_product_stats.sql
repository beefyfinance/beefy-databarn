-- Fails when fact_product_stats_1h misses the recent hourly share price of a product, or differs from the last 5-minute
-- price of the hour of its share token in fact_asset_price_5m (token.asset_id).
-- Window: the last var share_price_propagation_days days, up to the last full hour of fact_asset_price_5m.

{% set window_days = var('share_price_propagation_days', 7) %}

with anchor as (
  select toStartOfHour(max(date_time)) as window_end
  from {{ ref('fact_asset_price_5m') }}
),

shares as (
  select
    t.chain_id as chain_id,
    t.representation_address as product_address,
    toStartOfHour(p.date_time) as date_hour,
    argMax(p.price, p.date_time) as share_price
  from {{ ref('fact_asset_price_5m') }} p
  inner join (
    select t.chain_id, t.representation_address, assumeNotNull(t.asset_id) as asset_id
    from {{ ref('dim_token') }} t
    inner join {{ ref('int_product_keys') }} pk
      on t.chain_id = pk.chain_id
      and t.representation_address = pk.product_address
    where t.asset_id is not null
  ) t
    on p.asset_id = t.asset_id
  where p.date_time >= (select window_end from anchor) - interval {{ window_days }} day
    and p.date_time < (select window_end from anchor)
  group by chain_id, product_address, date_hour
),

stats as (
  select chain_id, product_address, date_hour, cast(toFloat64(share_token_price_usd) as Nullable(Float64)) as stats_price
  from {{ ref('fact_product_stats_1h') }}
  where date_hour >= (select window_end from anchor) - interval {{ window_days }} day
)

select s.chain_id as chain_id, s.product_address as product_address, s.date_hour as date_hour, s.share_price as share_price, st.stats_price as stats_price
from shares s
left join stats st
  on s.chain_id = st.chain_id
  and s.product_address = st.product_address
  and s.date_hour = st.date_hour
where st.stats_price is null
  -- fact_product_stats_1h stores Decimal(76, 20): prices under 1e-14 lose relative precision to truncation.
  or abs(st.stats_price - s.share_price) > greatest(1e-6 * abs(s.share_price), 1e-20)
