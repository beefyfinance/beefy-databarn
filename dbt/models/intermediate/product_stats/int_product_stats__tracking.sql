{{
  config(
    materialized='table',
    tags=['intermediate', 'fact_product_stats_1h'],
    engine='MergeTree()',
    order_by=['chain_id', 'product_address'],
  )
}}

-- When product stats stop: 3 months after the product retires or its chain goes eol, whichever comes first.
-- Grain is (chain_id, product_address), every product of int_product_keys. tracked_until is null while neither
-- has happened. Read by macros/product_is_tracked.sql, the HAVING of the hourly snapshot models (tvl, apy,
-- apy_breakdown, lps_breakdown, share_price). Rows already written are kept: the cutoff only stops new ones.
-- Why: the Beefy API keeps serving retired vaults (/apy/breakdown, /lps/breakdown) with their last values frozen,
-- so without a cutoff an eol vault would show an APY forever.

{% set never = "toDateTime('2106-01-01 00:00:00', 'UTC')" %}

with retired as (
  -- The API sets retired_at (unix seconds) on eol vaults, CLMs and reward pools.
  select c.chain_id as chain_id, v.earn_contract_address as product_address,
    cast(toDateTime(v.retired_at, 'UTC') as Nullable(DateTime('UTC'))) as retired_at
  from {{ ref('stg_beefy_api__vaults') }} v
  inner join {{ ref('int_chain_keys') }} c on v.network = c.beefy_key
  where v.status = 'eol' and v.retired_at > 0
  union all
  select c.chain_id, v.earn_contract_address, cast(toDateTime(v.retired_at, 'UTC') as Nullable(DateTime('UTC')))
  from {{ ref('stg_beefy_api__clm_vaults') }} v
  inner join {{ ref('int_chain_keys') }} c on v.network = c.beefy_key
  where v.status = 'eol' and v.retired_at > 0
  union all
  select c.chain_id, v.earn_contract_address, cast(toDateTime(v.retired_at, 'UTC') as Nullable(DateTime('UTC')))
  from {{ ref('stg_beefy_api__gov_vaults') }} v
  inner join {{ ref('int_chain_keys') }} c on v.network = c.beefy_key
  where v.status = 'eol' and v.retired_at > 0
  union all
  -- Gone from the API: databarn v1's eol date.
  select chain_id, product_address, retired_at
  from {{ ref('int_databarn_v1__retired_vaults') }}
  where retired_at is not null
),

retired_products as (
  select chain_id, product_address, min(retired_at) as retired_at
  from retired
  group by chain_id, product_address
),

-- A boost ends at period_finish: its status stays 'active' long after (2026-11: 503 of 508 "active" boosts had
-- finished). A boost also ends with the vault it stakes (joined below).
boosts as (
  select c.chain_id as chain_id, b.boost_contract_address as product_address,
    cast(max(toDateTime(b.period_finish, 'UTC')) as Nullable(DateTime('UTC'))) as boost_ended_at
  from {{ ref('stg_beefy_api__boosts') }} b
  inner join {{ ref('int_chain_keys') }} c on b.chain = c.beefy_key
  where (b.version is null or b.version < 2) and b.period_finish > 0
  group by chain_id, product_address
),

chains as (
  select network_id as chain_id, cast(eol_date_time as Nullable(DateTime('UTC'))) as chain_eol_at
  from {{ ref('stg_github_files___beefy_ui_chains') }}
  where eol_date_time is not null
),

products as (
  select
    p.chain_id as chain_id,
    p.product_address as product_address,
    -- the boost's own end, or the end of the vault it stakes
    nullIf(least(
      ifNull(r.retired_at, {{ never }}),
      ifNull(b.boost_ended_at, {{ never }}),
      ifNull(staked.retired_at, {{ never }})
    ), {{ never }}) as retired_at,
    ch.chain_eol_at as chain_eol_at
  from {{ ref('int_product_keys') }} p
  left join retired_products r on r.chain_id = p.chain_id and r.product_address = p.product_address
  left join boosts b on b.chain_id = p.chain_id and b.product_address = p.product_address
  left join retired_products staked
    on staked.chain_id = p.chain_id and staked.product_address = p.boost_staked_product_address
  left join chains ch on ch.chain_id = p.chain_id
)

select
  chain_id,
  product_address,
  retired_at,
  chain_eol_at,
  cast(
    nullIf(least(ifNull(retired_at, {{ never }}), ifNull(chain_eol_at, {{ never }})), {{ never }}) + INTERVAL 3 MONTH
    as Nullable(DateTime('UTC'))
  ) as tracked_until
-- Every date column is Nullable, so a product missing from a LEFT JOIN gets NULL, not the epoch.
from products
