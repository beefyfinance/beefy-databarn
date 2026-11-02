-- Fails when an asset has prices in a month of asset_price_5m but not in asset_price_1h, or in asset_price_1h but
-- not in asset_price_1d: these three are reimported separately, month by month.
-- Not the current month: its last hours can still be loading.

with five_minutes as (
  select distinct asset_id, toStartOfMonth(date_time) as month
  from {{ ref('asset_price_5m') }}
  where date_time < toStartOfMonth(now())
),

hours as (
  select distinct asset_id, toStartOfMonth(date_hour) as month
  from {{ ref('asset_price_1h') }}
  where date_hour < toStartOfMonth(now())
),

days as (
  select distinct asset_id, toStartOfMonth(date_day) as month
  from {{ ref('asset_price_1d') }}
)

select 'asset_price_1h' as missing_from, f.asset_id as asset_id, f.month as month
from five_minutes f
left anti join hours h
  on f.asset_id = h.asset_id
  and f.month = h.month

union all

select 'asset_price_1d' as missing_from, h.asset_id as asset_id, h.month as month
from hours h
left anti join days d
  on h.asset_id = d.asset_id
  and h.month = d.month
