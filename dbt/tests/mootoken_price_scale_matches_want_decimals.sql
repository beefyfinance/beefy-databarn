-- Fails when a recent /mootokenprices share price of a non-18-decimal vault is not on the 1e18 share unit scale.
-- The vault's own ppfs (want raw units per 1e18 share units) gives the expected share price, so ratio should be
-- near 1. It is far off, by at least 1e2 and usually 1e10, when var mootoken_price_fix_deployed_at is wrong:
-- the snapshot is rescaled twice or not at all.
-- Ppfs, not 10^(18 - want decimals): ERC-4626 vaults (silov2, venus) mint 18-decimal shares, not 1:1 with want units.
-- Listed vaults only: /mootokenprices keeps pricing some vaults /vaults dropped (venus-arb-wbtc, about 1e10 off),
-- and their last /vaults row is stale.

with shares as (
  -- Share prices observed from /mootokenprices in the last day (not carried slots), by share token.
  select a.chain_id as chain_id, a.share_token_address as product_address, argMax(p.price, p.date_time) as share_price
  from {{ ref('fact_asset_price_5m') }} p
  inner join (
    select asset_id, assumeNotNull(chain_id) as chain_id, assumeNotNull(share_token_address) as share_token_address
    from {{ ref('dim_asset') }}
    where asset_source = 'beefy_api_share'
  ) a
    on p.asset_id = a.asset_id
  where p.price_source = 'api'
    and p.is_carried = 0
    and p.date_time >= now() - interval 1 day
  group by chain_id, product_address
),

wants as (
  select asset_id, argMax(price, date_time) as want_price
  from {{ ref('fact_asset_price_5m') }}
  where date_time >= now() - interval 2 day
    and asset_id in (
      select {{ asset_id_from_beefy_price_oracle_id('beefy_wants_price_oracle_id') }} from {{ ref('int_product_keys') }}
      where beefy_wants_price_oracle_id is not null
    )
  group by asset_id
)

select
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  pk.beefy_key as beefy_key,
  v.token_decimals as want_decimals,
  toFloat64OrNull(v.price_per_full_share) as ppfs,
  toFloat64(s.share_price) as share_price,
  toFloat64(w.want_price) as want_price,
  toFloat64(s.share_price) / (toFloat64(w.want_price) * ppfs / pow(10, v.token_decimals)) as ratio
from shares s
inner join {{ ref('int_product_keys') }} pk
  on s.chain_id = pk.chain_id
  and s.product_address = pk.product_address
inner join {{ ref('stg_beefy_api__vaults') }} v
  on pk.beefy_key = v.id
inner join wants w
  on w.asset_id = {{ asset_id_from_beefy_price_oracle_id('pk.beefy_wants_price_oracle_id') }}
where v.token_decimals != 18
  and v.is_listed
  and s.share_price > 0
  and w.want_price > 0
  and ppfs > 0
  and ratio not between 0.1 and 10
