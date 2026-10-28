{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['dimension', 'tokens', 'prices'],
    order_by=['asset_id'],
  )
}}

-- Dimension table: price series. One row per asset; tokens link to it with token.asset_id, so the same
-- token on several chains shares one price series. asset_source, in priority order:
--   beefy_db_oracle:  one asset per beefy_db price oracle (prices from int_beefy_oracle_price).
--                     An oracle named after a vault id prices that vault's want token, NOT its share token.
--   beefy_api_share:  one asset per vault share token with no beefy_db oracle (prices from int_product_share_price).
-- '-rp' oracles duplicate their base oracle and are left out (as in int_beefy_oracle_price).

with token_counts as (
  select
    asset_id,
    argMin(symbol, (token_kind, chain_id)) as symbol,
    -- Kinds of the linked token contracts (one token can be an erc20 on one chain and a vault want elsewhere).
    arraySort(groupUniqArray(token_kind)) as asset_kinds,
    count() as token_count,
    uniqExact(chain_id) as chain_count
  from {{ ref('token') }}
  where asset_id is not null
  group by asset_id
),

oracle_assets as (
  select
    {{ asset_id_from_beefy_price_oracle_id('o.id') }} as asset_id,
    'beefy_db_oracle' as asset_source,
    cast(o.id as Nullable(String)) as beefy_price_oracle_id,
    cast(o.oracle_id as Nullable(String)) as beefy_price_oracle_key,
    cast(null as Nullable(Int64)) as chain_id,
    cast(null as Nullable(String)) as share_token_address
  from {{ ref('stg_beefy_db__price_oracles') }} o
  where not endsWith(o.oracle_id, '-rp')
),

share_assets as (
  select
    assumeNotNull(asset_id) as asset_id,
    'beefy_api_share' as asset_source,
    cast(null as Nullable(String)) as beefy_price_oracle_id,
    cast(null as Nullable(String)) as beefy_price_oracle_key,
    cast(chain_id as Nullable(Int64)) as chain_id,
    cast(representation_address as Nullable(String)) as share_token_address
  from {{ ref('token') }}
  where asset_id = {{ asset_id_from_share_token('chain_id', 'representation_address') }}
),

assets as (
  select * from oracle_assets
  union all
  select * from share_assets
)

select
  a.asset_id as asset_id,
  a.asset_source as asset_source,
  cast(tc.asset_kinds as Array(LowCardinality(String))) as asset_kinds,
  cast(coalesce(nullIf(tc.symbol, ''), a.beefy_price_oracle_key, 'Unknown') as String) as symbol,
  a.beefy_price_oracle_id as beefy_price_oracle_id,
  a.beefy_price_oracle_key as beefy_price_oracle_key,
  a.chain_id as chain_id,
  a.share_token_address as share_token_address,
  toUInt64(ifNull(tc.token_count, 0)) as token_count,
  toUInt64(ifNull(tc.chain_count, 0)) as chain_count
from assets a
left join token_counts tc
  on a.asset_id = tc.asset_id
