-- Fails when a databarn v1 vault has ppfs and a priced want on a day, but its share token has no price that month
-- in fact_asset_price_1d: the databarn share series lost it (want decimals, product keys, the want oracle or the series
-- map). Whole months, so this stays cheap over the full history. Not the current month: it can still be loading.

with ppfs as (
  select product_id, toDate(datetime) as day
  from {{ ref('stg_databarn_v1__vault_stats') }}
  where share_to_underlying_price > 0
    and datetime >= '2021-08-01'
  group by product_id, day

  union distinct

  select assumeNotNull(s.product_id) as product_id, toDate(r.datetime) as day
  from {{ ref('stg_databarn_v1__prices_raw') }} r
  inner join {{ ref('stg_databarn_v1__import_states') }} s
    on r.price_feed_id = s.price_feed_id
    and s.import_type = 'product:share-rate'
  where r.price > 0
    and r.datetime >= '2021-08-01'
  group by product_id, day
),

-- ppfs days of each product with its want asset.
keyed as (
  select distinct
    pk.chain_id as chain_id,
    pk.product_address as product_address,
    pk.beefy_key as beefy_key,
    p.day as day,
    {{ asset_id_from_beefy_price_oracle_id('pk.beefy_wants_price_oracle_id') }} as want_asset_id
  from ppfs p
  inner join {{ ref('int_databarn_v1__want_decimals') }} wd
    on p.product_id = wd.product_id
  inner join {{ ref('int_product_keys') }} pk
    on wd.chain_id = pk.chain_id
    and wd.product_address = pk.product_address
  where pk.beefy_wants_price_oracle_id is not null
    and p.day < toStartOfMonth(now())
),

-- Months a product's share token has a price.
shares as (
  select distinct pa.chain_id as chain_id, pa.product_address as product_address, toStartOfMonth(p.date_day) as month
  from {{ ref('fact_asset_price_1d') }} p
  inner join {{ ref('int_product_price_assets') }} pa
    on p.asset_id = pa.asset_id
    and pa.role = 'share'
  where p.close > 0
)

-- Days the want has a price, by key lookup on fact_asset_price_1d.
select distinct k.chain_id as chain_id, k.product_address as product_address, k.beefy_key as beefy_key, toStartOfMonth(k.day) as month
from keyed k
left anti join shares s
  on k.chain_id = s.chain_id
  and k.product_address = s.product_address
  and toStartOfMonth(k.day) = s.month
where (k.want_asset_id, k.day) in (
  select asset_id, date_day from {{ ref('fact_asset_price_1d') }}
  where close > 0 and asset_id in (select want_asset_id from keyed)
)
