{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['chain_id', 'product_address', 'date_hour'],
  )
}}

-- Hardcoded product TVL (USD) corrections for beefy_db.tvls. Fabricated from the `windows` below only:
-- no source tables, so it is correct by construction.
-- beefy-db computes TVL with its own prices at write time, so a misprice leaks into tvl_usd and is not
-- always fixed by a reimport.
-- Grain is (chain_id, product_address, date_hour), the grain of int_product_stats__tvl_hourly,
-- which unions these rows with a higher priority than the hourly raw TVL.
--
-- HOW TO EDIT: add a row to `windows` with the product (chain_id, beefy_key for review, product_address),
-- the last good hour before the bad period (from_hour, from_tvl_usd) and the first good hour after it
-- (to_hour, to_tvl_usd), both UTC hour starts. One row is generated per hour strictly between the
-- anchors, linearly interpolated.
--
-- Removing a window does not delete corrected rows already written to int_product_stats__tvl_hourly:
-- reimport it (make dbt reimport 'int_product_stats__tvl_hourly+' <since>) or full refresh.

with windows as (
  select
    chain_id,
    beefy_key,
    product_address,
    toStartOfHour(toDateTime(from_hour, 'UTC')) as from_hour,
    from_tvl_usd,
    toStartOfHour(toDateTime(to_hour, 'UTC')) as to_hour,
    to_tvl_usd
  from values(
    'chain_id Int64, beefy_key String, product_address String, from_hour String, from_tvl_usd Float64, to_hour String, to_tvl_usd Float64',
    -- PYPLc incident: aerodrome-cow-base-usdc-pyplc-vault TVL bad 2026-10-22 21:00 to 2026-10-23 06:00.
    -- Anchors from dbt/analyses/price_corrections_2026_10_23_0715.sql.
    (8453, 'aerodrome-cow-base-usdc-pyplc-vault', '0x7818b61cfa261d9940173698a15fedb2dc238fab', '2026-10-22 20:00:00', 18221.092274430808, '2026-10-23 07:00:00', 18555.053845941093)
  )
),

points as (
  select
    chain_id,
    beefy_key,
    product_address,
    from_hour,
    from_tvl_usd,
    to_hour,
    to_tvl_usd,
    toDateTime(
      arrayJoin(
        range(
          toUInt32(toUnixTimestamp(from_hour) + 3600),
          toUInt32(toUnixTimestamp(to_hour)),
          3600
        )
      ),
      'UTC'
    ) as date_hour
  from windows
)

select
  chain_id,
  beefy_key,
  product_address,
  date_hour,
  from_tvl_usd + (to_tvl_usd - from_tvl_usd)
    * (toUnixTimestamp(date_hour) - toUnixTimestamp(from_hour))
    / (toUnixTimestamp(to_hour) - toUnixTimestamp(from_hour)) as tvl_usd,
  cast('interpolated' as String) as tvl_source,
  from_tvl_usd,
  from_hour,
  to_tvl_usd,
  to_hour
from points
