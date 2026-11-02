-- Fails when the recent hourly close of a share price asset is missing or differs from the share price.
-- Only assets priced by their share series: a CLM manager or an oracle-priced token has its own series.
-- Window: see share_price_reaches_product_stats.

{% set window_days = var('share_price_propagation_days', 7) %}

with anchor as (
  select max(date_time) - interval 1 hour as window_end
  from {{ ref('int_product_share_price') }}
),

shares as (
  select
    a.asset_id as asset_id,
    toStartOfHour(p.date_time) as date_hour,
    toFloat64(argMax(p.share_token_price_usd, p.date_time)) as share_price
  from {{ ref('int_product_share_price') }} p FINAL
  inner join (
    select asset_id, assumeNotNull(chain_id) as chain_id, assumeNotNull(share_token_address) as share_token_address
    from {{ ref('asset') }}
    where asset_source = 'beefy_api_share'
  ) a
    on p.chain_id = a.chain_id
    and p.product_address = a.share_token_address
  where p.date_time >= (select window_end from anchor) - interval {{ window_days }} day
    and p.date_time < (select toStartOfHour(window_end) from anchor)
  group by asset_id, date_hour
),

prices as (
  select asset_id, date_hour, cast(toFloat64(close) as Nullable(Float64)) as asset_close
  from {{ ref('asset_price_1h') }}
  where date_hour >= (select window_end from anchor) - interval ({{ window_days }} + 1) day
)

select
  s.asset_id as asset_id,
  s.date_hour as date_hour,
  s.share_price as share_price,
  p.asset_close as asset_close
from shares s
left join prices p
  on s.asset_id = p.asset_id
  and s.date_hour = p.date_hour
where p.asset_close is null
  or abs(p.asset_close - s.share_price) > 1e-6 * abs(s.share_price)
