{{
  config(
    materialized='table',
    tags=['intermediate'],
    engine='MergeTree()',
    order_by=['beefy_price_oracle_key', 'date_time'],
  )
}}

-- Hardcoded Beefy API oracle price corrections (/prices and /lps snapshots). Fabricated from the
-- `windows` below only: no source tables, so it is correct by construction.
-- Oracle prices (wants tokens, native gas tokens), not vault shares. Consumed by int_harvests and
-- int_product_stats__lps_breakdown_hourly.
-- Grain is (beefy_price_oracle_key, date_time), on the 5-minute grid of the beefy_api dlt
-- snapshots (infra/dlt/scheduler.py). A consumer must bucket raw snapshots with toStartOfFiveMinutes
-- and union these rows with a higher priority.
-- beefy_db prices are not corrected here: they are fixed upstream and reimported (make reimport-prices).
--
-- HOW TO EDIT: add a row to `windows` with the numeric db oracle id (stg_beefy_db__price_oracles.id, for review),
-- the oracle key (stg_beefy_db__price_oracles.oracle_id:
-- token symbol or LP vault id), the last good price before the bad window (from_time, from_price)
-- and the first good price after it (to_time, to_price), both UTC. One row is generated per
-- 5-minute slot strictly between the anchors, linearly interpolated.

with windows as (
  select
    beefy_price_oracle_id,
    beefy_price_oracle_key,
    toDateTime(from_time, 'UTC') as from_time,
    from_price,
    toDateTime(to_time, 'UTC') as to_time,
    to_price
  from values(
    'beefy_price_oracle_id String, beefy_price_oracle_key String, from_time String, from_price Float64, to_time String, to_price Float64',
    -- PYPLc incident (beefy-db oracle ids 8997, 8999, 9000): API prices intermittently absurd (up to ~1e40 USD)
    -- from 2026-10-22 21:20 to 2026-10-23 07:30. Anchors from dbt/analyses/price_corrections_2026_10_23_0715.sql.
    ('8997', 'PYPLc', '2026-10-22 21:10:00', 55.09827960514487, '2026-10-23 07:45:00', 58.841851618987555),
    ('8999', 'aerodrome-cow-base-usdc-pyplc', '2026-10-22 21:15:00', 555817276739.2109, '2026-10-23 07:45:00', 566004606719.9084),
    ('9000', 'aerodrome-cow-base-usdc-pyplc-rp', '2026-10-22 21:15:00', 555817276739.2109, '2026-10-23 07:45:00', 566004606719.9084)
  )
),

points as (
  select
    beefy_price_oracle_id,
    beefy_price_oracle_key,
    from_time,
    from_price,
    to_time,
    to_price,
    toDateTime(
      arrayJoin(
        range(
          toUInt32(toUnixTimestamp(toStartOfFiveMinutes(from_time)) + 300),
          toUInt32(toUnixTimestamp(toStartOfFiveMinutes(to_time))),
          300
        )
      ),
      'UTC'
    ) as date_time
  from windows
)

select
  beefy_price_oracle_id,
  beefy_price_oracle_key,
  date_time,
  cast(
    from_price + (to_price - from_price)
      * (toUnixTimestamp(date_time) - toUnixTimestamp(from_time))
      / (toUnixTimestamp(to_time) - toUnixTimestamp(from_time))
    as Decimal(76, 20)
  ) as price,
  cast('interpolated' as String) as price_source,
  cast(from_price as Decimal(76, 20)) as from_price,
  from_time,
  cast(to_price as Decimal(76, 20)) as to_price,
  to_time
from points
