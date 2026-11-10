-- Fails when a product's share token has prices in a month of fact_asset_price_5m but fact_product_stats_1h has none, or
-- fact_asset_price_1d has none: a partial reimport or a missed recompute leaves such holes.
-- Whole months, so this stays cheap over the full history. fact_product_stats_1h starts at 2021-07-31.

with share_assets as (
  select asset_id, assumeNotNull(chain_id) as chain_id, assumeNotNull(share_token_address) as share_token_address
  from {{ ref('dim_asset') }}
  where asset_source = 'beefy_api_share'
),

-- Months a product's share token has prices in fact_asset_price_5m.
shares as (
  select a.chain_id as chain_id, a.share_token_address as product_address, p.month as month
  from (
    select distinct asset_id, toStartOfMonth(date_time) as month
    from {{ ref('fact_asset_price_5m') }}
    where asset_id in (select asset_id from share_assets)
      -- A share of a dead vault can be priced 0: fact_product_stats_1h needs a positive price.
      and price > 0
      and date_time >= '2021-08-01'
      and toStartOfMonth(date_time) < toStartOfMonth(now())
  ) p
  inner join share_assets a
    on p.asset_id = a.asset_id
  inner join {{ ref('int_product_keys') }} pk
    on a.chain_id = pk.chain_id
    and a.share_token_address = pk.product_address
),

stats as (
  select distinct chain_id, product_address, toStartOfMonth(date_hour) as month
  from {{ ref('fact_product_stats_1h') }}
  where share_price_usd > 0
),

prices as (
  select distinct asset_id, toStartOfMonth(date_day) as month
  from {{ ref('fact_asset_price_1d') }}
)

select 'fact_product_stats_1h' as missing_from, s.chain_id as chain_id, s.product_address as product_address, s.month as month
from shares s
left anti join stats st
  on s.chain_id = st.chain_id
  and s.product_address = st.product_address
  and s.month = st.month

union all

select 'fact_asset_price_1d' as missing_from, s.chain_id as chain_id, s.product_address as product_address, s.month as month
from shares s
inner join share_assets a
  on s.chain_id = a.chain_id
  and s.product_address = a.share_token_address
left anti join prices p
  on a.asset_id = p.asset_id
  and s.month = p.month
