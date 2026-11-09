-- Fails when fact_asset_price_1d misses a day of fact_asset_price_5m, or disagrees with it on the number of 5-minute prices:
-- fact_asset_price_1d rebuilds the days each resolver run touched (int_price_runs), so a missed run shows here.
-- Closed days of the last 30 days (older days were checked when they were recent).

with five_minutes as (
  select asset_id, toDate(date_time) as date_day, toUInt32(count()) as samples
  from {{ ref('fact_asset_price_5m') }}
  where date_time >= toStartOfDay(now()) - interval 30 day
    and date_time < toStartOfDay(now())
  group by asset_id, date_day
),

days as (
  select asset_id, date_day, sample_count
  from {{ ref('fact_asset_price_1d') }}
  where date_day >= toDate(now()) - 30
    and date_day < toDate(now())
)

select f.asset_id as asset_id, f.date_day as date_day, f.samples as samples, d.sample_count as sample_count
from five_minutes f
left join days d
  on f.asset_id = d.asset_id
  and f.date_day = d.date_day
where d.sample_count != f.samples
