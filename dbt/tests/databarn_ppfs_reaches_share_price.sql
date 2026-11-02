-- Fails when a databarn v1 vault has ppfs and a want price in a month, but int_product_share_price has no price
-- for it that month: the join on want decimals, product keys or the want oracle lost it.
-- Whole months, so this stays cheap over the full history. Not the current month: it can still be loading.

with ppfs as (
  select product_id, toStartOfMonth(datetime) as month
  from {{ ref('stg_databarn_v1__vault_stats') }}
  where share_to_underlying_price > 0
    and datetime >= '2021-08-01'
  group by product_id, month

  union distinct

  select assumeNotNull(s.product_id) as product_id, toStartOfMonth(r.datetime) as month
  from {{ ref('stg_databarn_v1__prices_raw') }} r
  inner join {{ ref('stg_databarn_v1__import_states') }} s
    on r.price_feed_id = s.price_feed_id
    and s.import_type = 'product:share-rate'
  where r.price > 0
    and r.datetime >= '2021-08-01'
  group by product_id, month
),

wants as (
  select distinct beefy_price_oracle_key, toStartOfMonth(date_time) as month
  from {{ ref('int_beefy_oracle_price') }}
),

shares as (
  select distinct chain_id, product_address, toStartOfMonth(date_time) as month
  from {{ ref('int_product_share_price') }}
)

select
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  pk.beefy_key as beefy_key,
  p.month as month
from ppfs p
inner join {{ ref('int_databarn_v1__want_decimals') }} wd
  on p.product_id = wd.product_id
inner join {{ ref('int_product_keys') }} pk
  on wd.chain_id = pk.chain_id
  and wd.product_address = pk.product_address
inner join wants w
  on pk.beefy_wants_price_oracle_key = w.beefy_price_oracle_key
  and p.month = w.month
left anti join shares s
  on pk.chain_id = s.chain_id
  and pk.product_address = s.product_address
  and p.month = s.month
where p.month < toStartOfMonth(now())
