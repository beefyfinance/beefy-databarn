-- Fails when a product has share prices in a month but product_stats has none, or when a share price asset has
-- share prices in a month but asset_price_1d has none: a partial reimport leaves such holes.
-- Whole months, so this stays cheap over the full history. product_stats starts at 2021-07-31.

with shares as (
  select distinct chain_id, product_address, toStartOfMonth(date_time) as month
  from {{ ref('int_product_share_price') }}
  where date_time >= '2021-08-01'
    and toStartOfMonth(date_time) < toStartOfMonth(now())
    and share_token_price_usd > 0
),

stats as (
  select distinct chain_id, product_address, toStartOfMonth(date_hour) as month
  from {{ ref('product_stats') }}
  where share_token_price_usd > 0
),

share_assets as (
  select asset_id, assumeNotNull(chain_id) as chain_id, assumeNotNull(share_token_address) as share_token_address
  from {{ ref('asset') }}
  where asset_source = 'beefy_api_share'
),

prices as (
  select distinct asset_id, toStartOfMonth(date_day) as month
  from {{ ref('asset_price_1d') }}
)

select 'product_stats' as missing_from, s.chain_id as chain_id, s.product_address as product_address, s.month as month
from shares s
left anti join stats st
  on s.chain_id = st.chain_id
  and s.product_address = st.product_address
  and s.month = st.month

union all

select 'asset_price_1d' as missing_from, s.chain_id as chain_id, s.product_address as product_address, s.month as month
from shares s
inner join share_assets a
  on s.chain_id = a.chain_id
  and s.product_address = a.share_token_address
left anti join prices p
  on a.asset_id = p.asset_id
  and s.month = p.month
