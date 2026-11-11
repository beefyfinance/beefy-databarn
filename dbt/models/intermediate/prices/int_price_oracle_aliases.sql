{{
  config(
    materialized='table',
    tags=['intermediate', 'prices'],
    engine='MergeTree()',
    order_by=['alias_key', 'alias_from'],
  )
}}

-- Hardcoded beefy_db oracle aliases: over [alias_from, alias_until) the oracle source_key prices the token of the
-- oracle alias_key. Fabricated from the values() below only: no source tables. int_price_series_map maps the source
-- oracle's series to the alias's asset over that window, so the alias reuses real observations of another key.
--   kind 'renamed': the same token was priced under an older key. beefy_db renamed oracle keys in batches and its old
--     key stops 15 minutes before the new one starts (2022-07-16 sushi-*, 2022-10-27 .e tokens, 2023-02-03 Stargate,
--     ...), so wants priced under the new key had no price before it. alias_until = the new key's first price.
--     price_source 'beefy_db_alias'.
--   kind 'underlying': no older key; the token is approximated by its underlying (a bridged or wrapped USDC, ETH,
--     BTC...). price_source 'underlying_fallback', so consumers can tell these rows apart.
-- replaces = 1 also ends alias_key's own series at alias_from (WROSE).
--
-- HOW TO EDIT: add a row; an edit re-resolves the affected assets on the next dbt run (fact_asset_price_5m diffs the
-- series map). Pick renames by the old key's last raw price matching the new key's first (dlt.beefy_db___prices)
-- and give each old key to one new key only.

SELECT
  alias_key,
  source_key,
  toDateTime(alias_from, 'UTC') AS alias_from,
  toDateTime(alias_until, 'UTC') AS alias_until,
  replaces,
  kind,
  note
FROM values(
  'alias_key String, source_key String, alias_from String, alias_until String, replaces UInt8, kind String, note String',
  -- Before the 2023 BIFI migration the BIFI oracle priced the token oldBIFI names.
  ('oldBIFI', 'BIFI', '1970-01-01 00:00:00', '2023-10-24 13:45:00', 0, 'renamed', 'BIFI migration'),
  -- WROSE (the Oasis native token): from 2022-11-09 beefy_db's WROSE oracle tracks Rose Finance's ROSE on Aurora
  -- (~$0.001 against ~$0.05); its wROSE oracle keeps the Oasis price until it freezes on 2023-11-29 (Oasis harvests
  -- end 2023-08). After that WROSE carries wROSE's last price.
  ('WROSE', 'wROSE', '2022-11-09 00:00:00', '2023-11-29 10:05:00', 1, 'renamed', 'WROSE tracks Rose Finance from 2022-11-09'),
   -- Renamed keys, from the old key's last raw price matching the new key's first (2026-11-11).
  ('stargate-arb-usdc', 'sarUSDC', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-arb-usdt', 'sarUSDT', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-arb-eth', 'sarETH', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-avax-usdc', 'saUSDC', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-avax-usdt', 'saUSDT', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-bsc-busd', 'sbBUSD', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-bsc-usdt', 'sbUSDT', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-eth-eth', 'sethETH', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-eth-usdc', 'sethUSDC', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-eth-usdt', 'sethUSDT', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-fantom-usdc', 'sfUSDC', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-op-usdc', 'soUSDC', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-op-eth', 'soETH', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-polygon-usdc', 'spUSDC', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('stargate-polygon-usdt', 'spUSDT', '1970-01-01 00:00:00', '2023-02-03 17:00:00', 0, 'renamed', 'Stargate rename 2023-02-03'),
  ('USDCe', 'USDC.e', '1970-01-01 00:00:00', '2022-10-27 22:15:00', 0, 'renamed', 'Avalanche .e rename 2022-10-27'),
  ('WBTCe', 'WBTC.e', '1970-01-01 00:00:00', '2022-10-27 22:15:00', 0, 'renamed', 'Avalanche .e rename 2022-10-27'),
  ('mdex-rabbit-busd', 'mdex-bsc-rabbit-busd', '1970-01-01 00:00:00', '2021-12-30 07:45:00', 0, 'renamed', 'mdex rename 2021-12-30'),
  ('hector-tor-crv', 'curve-ftm-tor', '1970-01-01 00:00:00', '2022-05-24 16:30:00', 0, 'renamed', 'Curve TOR pool rename 2022-05-24'),
  ('voltage-fusd-bnb', 'fusefi-fusd-bnb', '1970-01-01 00:00:00', '2022-02-03 13:45:00', 0, 'renamed', 'FuseFi became Voltage 2022-02-03'),
  ('voltage-wbtc-weth', 'fusefi-wbtc-weth', '1970-01-01 00:00:00', '2022-02-03 13:45:00', 0, 'renamed', 'FuseFi became Voltage 2022-02-03'),
  ('voltage-wfuse-busd', 'fusefi-wfuse-busd', '1970-01-01 00:00:00', '2022-02-03 13:45:00', 0, 'renamed', 'FuseFi became Voltage 2022-02-03'),
  ('voltage-wfuse-fusd', 'fusefi-wfuse-fusd', '1970-01-01 00:00:00', '2022-02-03 13:45:00', 0, 'renamed', 'FuseFi became Voltage 2022-02-03'),
  ('voltage-wfuse-g$', 'fusefi-wfuse-g$', '1970-01-01 00:00:00', '2022-02-03 13:45:00', 0, 'renamed', 'FuseFi became Voltage 2022-02-03'),
  ('voltage-wfuse-usdc', 'fusefi-wfuse-usdc', '1970-01-01 00:00:00', '2022-02-03 13:45:00', 0, 'renamed', 'FuseFi became Voltage 2022-02-03'),
  ('voltage-wfuse-weth', 'fusefi-wfuse-weth', '1970-01-01 00:00:00', '2022-02-03 13:45:00', 0, 'renamed', 'FuseFi became Voltage 2022-02-03'),
  ('sushi-poly-aave-eth', 'sushi-aave-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-avax-weth', 'sushi-avax-weth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-axs-eth', 'sushi-axs-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-bct-klima', 'sushi-bct-klima', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-eth-gohm', 'sushi-eth-gohm', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-eth-mana', 'sushi-eth-mana', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-eth-usdt', 'sushi-eth-usdt', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-eth-wfil', 'sushi-eth-wfil', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-eth2x-eth', 'sushi-eth2x-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-frax-fxs', 'sushi-frax-fxs', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-frax-usdc', 'sushi-frax-usdc', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-fxs-frax', 'sushi-fxs-frax', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-grt-eth', 'sushi-grt-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-jrt-eth', 'sushi-jrt-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-link-eth', 'sushi-link-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-matic-eth', 'sushi-matic-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-snx-eth', 'sushi-snx-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-usdc-bct', 'sushi-usdc-bct', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-usdc-bifi', 'sushi-usdc-bifi', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-usdc-dai', 'sushi-usdc-dai', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-usdc-eth', 'sushi-usdc-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-usdc-usdt', 'sushi-usdc-usdt', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-wbtc-ibbtc', 'sushi-wbtc-ibbtc', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-wmatic-ghst', 'sushi-wmatic-ghst', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-wmatic-woofy', 'sushi-wmatic-woofy', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-ftm-fusdt-wftm', 'sushi-fusdt-wftm', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16 (Fantom keys from 2022-01-23)'),
  ('sushi-ftm-usdc-mim', 'sushi-usdc-mim', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16 (Fantom keys from 2022-01-23)'),
  ('sushi-ftm-usdc-wftm', 'sushi-usdc-wftm', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16 (Fantom keys from 2022-01-23)'),
  ('sushi-ftm-wftm-eth', 'sushi-wftm-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16 (Fantom keys from 2022-01-23)'),
  ('sushi-ftm-wftm-link', 'sushi-wftm-link', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16 (Fantom keys from 2022-01-23)'),
  ('sushi-ftm-wftm-sushi', 'sushi-wftm-sushi', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16 (Fantom keys from 2022-01-23)'),
  ('sushi-ftm-yfi-eth', 'sushi-yfi-eth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16 (Fantom keys from 2022-01-23)'),
  ('sushi-fuse-wfuse-usdc', 'sushi-wfuse-usdc', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-fuse-wfuse-weth', 'sushi-wfuse-weth', '1970-01-01 00:00:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi chain prefix 2022-07-16'),
  ('sushi-poly-btc-eth', 'sushi-btc-eth', '1970-01-01 00:00:00', '2022-01-23 13:15:00', 0, 'renamed', 'sushi generic key, Polygon only until Fantom joined'),
  ('sushi-poly-btc-eth', 'polygon-sushi-btc-eth', '2022-03-28 06:45:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi Polygon key 2022-03-28..2022-07-16'),
  ('sushi-poly-eth-dai', 'sushi-eth-dai', '1970-01-01 00:00:00', '2022-01-23 13:15:00', 0, 'renamed', 'sushi generic key, Polygon only until Fantom joined'),
  ('sushi-poly-eth-dai', 'polygon-sushi-eth-dai', '2022-03-28 06:45:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi Polygon key 2022-03-28..2022-07-16'),
  ('sushi-ftm-btc-eth', 'sushi-btc-eth', '2022-01-23 13:15:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi generic key held the Fantom pool from 2022-01-23'),
  ('sushi-ftm-eth-dai', 'sushi-eth-dai', '2022-01-23 13:15:00', '2022-07-16 14:00:00', 0, 'renamed', 'sushi generic key held the Fantom pool from 2022-01-23'),
   -- Underlying fallbacks: no older key.
  ('fmcUSDC', 'USDC', '1970-01-01 00:00:00', '2023-07-06 00:00:00', 0, 'underlying', 'Multichain USDC on Fantom, at par until Multichain halted'),
  ('fmcETH', 'ETH', '1970-01-01 00:00:00', '2023-07-06 00:00:00', 0, 'underlying', 'Multichain ETH on Fantom, at par until Multichain halted'),
  ('fmcBTC', 'WBTC', '1970-01-01 00:00:00', '2023-07-06 00:00:00', 0, 'underlying', 'Multichain BTC on Fantom, at par until Multichain halted'),
  ('fmcDAI', 'DAI', '1970-01-01 00:00:00', '2023-07-06 00:00:00', 0, 'underlying', 'Multichain DAI on Fantom, at par until Multichain halted'),
  ('fmcCRV', 'CRV', '1970-01-01 00:00:00', '2023-07-06 00:00:00', 0, 'underlying', 'Multichain CRV on Fantom, at par until Multichain halted'),
  ('fmcLINK', 'LINK', '1970-01-01 00:00:00', '2023-07-06 00:00:00', 0, 'underlying', 'Multichain LINK on Fantom, at par until Multichain halted'),
  ('opUSDCe', 'USDC', '1970-01-01 00:00:00', '2023-09-13 10:15:00', 0, 'underlying', 'bridged USDC.e on Optimism, priced as USDC before opUSDCe existed'),
  ('pUSDCe', 'USDC', '1970-01-01 00:00:00', '2024-02-07 12:15:00', 0, 'underlying', 'bridged USDC.e on Polygon'),
  ('USDCn', 'USDC', '1970-01-01 00:00:00', '2025-07-24 19:30:00', 0, 'underlying', 'Noble USDC')
)
