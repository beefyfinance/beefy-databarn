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
-- Oracle prices (wants tokens, native gas tokens), not vault shares. Read by the price resolver (fact_asset_price_5m)
-- as a series of priority 90, on the 5-minute grid.
-- beefy_db prices are fixed upstream and reimported (make reimport-prices) when possible; these windows override
-- them too (priority 90 in fact_asset_price_5m), for glitches upstream keeps (joe-auto LPs, 2023-11). Edits recompute the
-- affected prices on the next dbt run.
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
    ('9000', 'aerodrome-cow-base-usdc-pyplc-rp', '2026-10-22 21:15:00', 555817276739.2109, '2026-10-23 07:45:00', 566004606719.9084),
    -- Trader Joe auto pool LPs (12 decimals): beefy_db prices 1e12 too high from 2023-11-12 14:30 to 2023-11-13 19:45.
    -- Anchors are the beefy_db prices just outside. joe-auto-usdc-eurc starts inside the window, so its left anchor
    -- repeats its first good price (EURC/USDC LP, stable).
    ('4374', 'joe-auto-wavax-usdc', '2023-11-12 14:15:00', 0.8236863394855967, '2023-11-13 20:00:00', 0.8144386209893207),
    ('4375', 'joe-auto-weth-usdc.e', '2023-11-12 14:15:00', 0.9318133792848493, '2023-11-13 20:00:00', 0.9444884811044745),
    ('4639', 'joe-auto-weth-usdc', '2023-11-12 14:15:00', 1.0362320719527431, '2023-11-13 20:00:00', 1.0505387310423053),
    ('4640', 'joe-auto-usdc-eurc', '2023-11-12 14:25:00', 0.9681418590157557, '2023-11-13 20:00:00', 0.9681418590157557)
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
