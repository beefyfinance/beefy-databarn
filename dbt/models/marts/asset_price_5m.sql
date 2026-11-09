{{
  config(
    materialized='price_resolver',
    tags=['marts', 'prices'],
  )
}}
-- depends_on: {{ ref('int_price_obs') }}
-- depends_on: {{ ref('int_price_series_map') }}
-- depends_on: {{ ref('int_price_oracle_corrections') }}
-- depends_on: {{ ref('int_mootoken_price_corrections') }}

-- Dense 5-minute USD price of every asset: one row per asset per 5-minute slot while a source is fresh. Written by
-- the price_resolver materialization (macros/materializations/price_resolver.sql); this select is not used.
-- Per slot: the highest-priority value of the slot (int_price_series_map), else the last value carried forward:
-- oracle values for 1 day, a vault's last ppfs at any age times its want price of the slot, other share values for
-- 1 day. The CLM subgraph is a fallback, used after an hour with no other value. observed_at is the slot of the value
-- a row holds or carries (its age without looking back: date_time - observed_at).
-- Price facts with the slot, not ASOF (an ASOF join holds the whole price side in memory):
--   LEFT JOIN asset_price_5m p ON p.asset_id = f.asset_id AND p.date_time = toStartOfFiveMinutes(f.ts)
-- and cut the price side to the facts' time range, or look keys up first:
--   WHERE (asset_id, date_time) IN (SELECT asset_id, toStartOfFiveMinutes(ts) FROM facts)
SELECT 1
