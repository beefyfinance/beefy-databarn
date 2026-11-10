{{
  config(
    materialized='table',
    engine='MergeTree',
    order_by=['series_id', 'asset_id'],
    tags=['intermediate', 'prices'],
  )
}}

-- Series map: every expected price series and the asset it prices, with every special case of the price flow as
-- columns of its row. Series are listed from the dimensions and hashed like the sources hash them
-- (macros/prices/price_sources.sql), so a series is mapped the run its product or oracle appears, whole history
-- included. fact_asset_price_5m diffs this table with the copy it applied last run and recomputes what changed.
-- Priorities: 90 corrections, 50 beefy_db / its alias / /mootokenprices, 40 databarn v1 ppfs, 10 CLM subgraph
-- (a fallback: used only after an hour with no value of priority 20 or more).
-- Correction series are read by fact_asset_price_5m from the correction models directly, not from int_price_obs.
-- Prices stored with facts are not sources: zap legs, buybacks and /lps/breakdown carry the beefy_db price already,
-- and harvest want prices are often wrong (2026-11: 6.7% more than 50% off beefy_db, many a placeholder 1 on CLM
-- wants). Facts are priced from this series and keep their stored price only where it has none.

WITH
  oracles AS (
    SELECT id, oracle_id AS oracle_key, {{ asset_id_from_beefy_price_oracle_id('id') }} AS asset_id
    FROM {{ ref('stg_beefy_db__price_oracles') }}
    -- '-rp' oracles duplicate their base oracle.
    WHERE NOT endsWith(oracle_id, '-rp')
  ),
  share_assets AS (
    SELECT asset_id, assumeNotNull(chain_id) AS chain_id, assumeNotNull(share_token_address) AS product_address
    FROM {{ ref('dim_asset') }}
    WHERE asset_source = 'beefy_api_share'
  ),
  -- Listed products that have a share asset, with the asset of their want token.
  products AS (
    SELECT
      pk.chain_id AS chain_id,
      pk.product_address AS product_address,
      pk.share_token_symbol AS share_token_symbol,
      c.beefy_key AS chain_key,
      s.asset_id AS asset_id,
      w.asset_id AS want_asset_id
    FROM {{ ref('int_product_keys') }} pk
    INNER JOIN share_assets s ON pk.chain_id = s.chain_id AND pk.product_address = s.product_address
    INNER JOIN {{ ref('int_chain_keys') }} c ON pk.chain_id = c.chain_id
    LEFT JOIN oracles w ON pk.beefy_wants_price_oracle_key = w.oracle_key
  ),
  -- databarn_v1_until: v1 ppfs is used until 1 month after the vault retired or its chain went eol
  -- (int_product_stats__retirement). v1 keeps reporting some retired vaults for years (2026-11: 105M observations
  -- past that bound, 626 series off by more than 1%, up to 3x), but stops importing most of them about a month after
  -- eol. After the bound the resolver carries the last v1 ppfs times the want price, unless another series prices it.
  databarn_products AS (
    SELECT wd.product_id AS product_id, wd.want_decimals AS want_decimals, p.asset_id AS asset_id, p.want_asset_id AS want_asset_id,
      ifNull(r.databarn_v1_until, toDateTime('2100-01-01 00:00:00', 'UTC')) AS valid_to
    FROM {{ ref('int_databarn_v1__want_decimals') }} wd
    INNER JOIN products p ON wd.chain_id = p.chain_id AND wd.product_address = p.product_address
    LEFT JOIN {{ ref('int_product_stats__retirement') }} r ON r.chain_id = p.chain_id AND r.product_address = p.product_address
    WHERE p.want_asset_id != 0
  ),
  share_rate_feeds AS (
    SELECT assumeNotNull(price_feed_id) AS price_feed_id, assumeNotNull(product_id) AS product_id
    FROM {{ ref('stg_databarn_v1__import_states') }}
    WHERE import_type = 'product:share-rate' AND price_feed_id IS NOT NULL AND product_id IS NOT NULL
  ),
  api_decimals AS (
    SELECT network, earned_token, argMax(token_decimals, created_at) AS token_decimals
    FROM {{ ref('stg_beefy_api__vaults') }}
    GROUP BY network, earned_token
  ),
  subgraph_series AS (
    SELECT chain_id, product_address, any(price_source) AS price_source
    FROM {{ ref('int_beefy_clm_subgraph__share_price_1h') }}
    GROUP BY chain_id, product_address
  ),
  -- An oracle that prices another oracle's token over [alias_from, alias_until). replaces = 1 also ends the
  -- destination's own series at alias_from.
  --   oldBIFI: before the 2023 BIFI migration the BIFI oracle priced the token oldBIFI names.
  --   WROSE (the Oasis native token): from 2022-11-09 beefy_db's WROSE oracle tracks Rose Finance's ROSE on Aurora
  --   (~$0.001 against ~$0.05); its wROSE oracle keeps the Oasis price until it freezes on 2023-11-29 (Oasis
  --   harvests end 2023-08). After that WROSE carries wROSE's last price.
  aliases AS (
    SELECT alias_key, source_key, toDateTime(alias_from, 'UTC') AS alias_from, toDateTime(alias_until, 'UTC') AS alias_until, replaces
    FROM values('alias_key String, source_key String, alias_from String, alias_until String, replaces UInt8',
      ('oldBIFI', 'BIFI', '1970-01-01 00:00:00', '2023-10-24 13:45:00', 0),
      ('WROSE', 'wROSE', '2022-11-09 00:00:00', '2023-11-29 10:05:00', 1))
  ),
  corr_oracle_keys AS (
    SELECT DISTINCT beefy_price_oracle_key FROM {{ ref('int_price_oracle_corrections') }}
  ),
  corr_share_keys AS (
    SELECT DISTINCT chain_id, product_address FROM {{ ref('int_mootoken_price_corrections') }}
  ),

  rows AS (
    -- beefy_db oracle -> its asset
    SELECT
      {{ price_series_id('beefy_db', 'o.id') }} AS series_id, 'beefy_db' AS source, o.asset_id AS asset_id,
      'oracle' AS asset_kind, 'usd' AS obs_kind, 50 AS priority, toUInt64(0) AS want_asset_id, nan AS ppfs_log10,
      1. AS scale, toDateTime(0, 'UTC') AS scale_until, toDateTime(0, 'UTC') AS valid_from,
      -- join_use_nulls = 0: an oracle no alias replaces gets replaces = 0.
      if(r.replaces = 1, r.alias_from, toDateTime('2100-01-01 00:00:00', 'UTC')) AS valid_to
    FROM oracles o
    LEFT JOIN (SELECT alias_key, alias_from, replaces FROM aliases WHERE replaces = 1) r ON r.alias_key = o.oracle_key

    UNION ALL
    -- Aliases (see aliases)
    SELECT
      {{ price_series_id('beefy_db', 'src.id') }}, 'beefy_db_alias', dst.asset_id, 'oracle', 'usd', 50, 0, nan, 1.,
      toDateTime(0, 'UTC'), a.alias_from, a.alias_until
    FROM aliases a
    INNER JOIN oracles src ON src.oracle_key = a.source_key
    INNER JOIN oracles dst ON dst.oracle_key = a.alias_key

    UNION ALL
    -- Oracle price corrections
    SELECT
      {{ price_series_id('corr_oracle', 'k.beefy_price_oracle_key') }}, 'corr_oracle', o.asset_id, 'oracle', 'usd', 90, 0,
      nan, 1., toDateTime(0, 'UTC'), toDateTime(0, 'UTC'), toDateTime('2100-01-01 00:00:00', 'UTC')
    FROM corr_oracle_keys k
    INNER JOIN oracles o ON k.beefy_price_oracle_key = o.oracle_key

    UNION ALL
    -- CLM subgraph -> the CLM's oracle asset (pool lp tokens and reward pools share it), fallback.
    SELECT
      {{ price_series_id('subgraph', 's.chain_id', 's.product_address') }}, 'clm_subgraph', o.asset_id, 'oracle', 'usd', 10,
      0, nan, 1., toDateTime(0, 'UTC'), toDateTime(0, 'UTC'), toDateTime('2100-01-01 00:00:00', 'UTC')
    FROM subgraph_series s
    INNER JOIN {{ ref('int_product_keys') }} pk ON s.chain_id = pk.chain_id AND s.product_address = pk.product_address
    INNER JOIN oracles o ON pk.beefy_share_price_oracle_key = o.oracle_key
    WHERE s.price_source = 'clm_subgraph'

    UNION ALL
    -- /mootokenprices -> share asset. Before the beefy-api fix the price was per 1e18 want units, not per share.
    SELECT
      {{ price_series_id('api', 'p.chain_key', 'p.share_token_symbol') }}, 'api', p.asset_id, 'share', 'usd', 50,
      p.want_asset_id, nan, pow(10, 18 - ifNull(d.token_decimals, 18)),
      toDateTime('{{ var("mootoken_price_fix_deployed_at") }}', 'UTC'), toDateTime(0, 'UTC'),
      toDateTime('2100-01-01 00:00:00', 'UTC')
    FROM products p
    LEFT JOIN api_decimals d ON p.chain_key = d.network AND p.share_token_symbol = d.earned_token

    UNION ALL
    -- Share price corrections
    SELECT
      {{ price_series_id('corr_share', 'k.chain_id', 'k.product_address') }}, 'corr_share', s.asset_id, 'share', 'usd', 90,
      ifNull(p.want_asset_id, 0), nan, 1., toDateTime(0, 'UTC'), toDateTime(0, 'UTC'),
      toDateTime('2100-01-01 00:00:00', 'UTC')
    FROM corr_share_keys k
    INNER JOIN share_assets s ON k.chain_id = s.chain_id AND k.product_address = s.product_address
    LEFT JOIN products p ON s.chain_id = p.chain_id AND s.product_address = p.product_address

    UNION ALL
    -- databarn v1 ppfs from vault stats -> share asset. Needs the want price.
    SELECT
      {{ price_series_id('dbv1_product', 'dp.product_id') }}, 'dbv1_product', dp.asset_id, 'share', 'ppfs', 40,
      dp.want_asset_id, toFloat64(18 - dp.want_decimals), 1., toDateTime(0, 'UTC'), toDateTime(0, 'UTC'),
      dp.valid_to
    FROM databarn_products dp

    UNION ALL
    -- databarn v1 share-rate feeds -> share asset (same ppfs, another feed).
    SELECT
      {{ price_series_id('dbv1_feed', 'f.price_feed_id') }}, 'dbv1_feed', dp.asset_id, 'share', 'ppfs', 40,
      dp.want_asset_id, toFloat64(18 - dp.want_decimals), 1., toDateTime(0, 'UTC'), toDateTime(0, 'UTC'),
      dp.valid_to
    FROM share_rate_feeds f
    INNER JOIN databarn_products dp ON f.product_id = dp.product_id

    UNION ALL
    -- CLM subgraph (CLM managers and classic vaults on a CLM) -> share asset, listed by the API or not, fallback.
    SELECT
      {{ price_series_id('subgraph', 's.chain_id', 's.product_address') }}, s.price_source, a.asset_id, 'share', 'usd', 10,
      ifNull(p.want_asset_id, 0), nan, 1., toDateTime(0, 'UTC'), toDateTime(0, 'UTC'),
      toDateTime('2100-01-01 00:00:00', 'UTC')
    FROM subgraph_series s
    INNER JOIN share_assets a ON s.chain_id = a.chain_id AND s.product_address = a.product_address
    LEFT JOIN products p ON a.chain_id = p.chain_id AND a.product_address = p.product_address
  )

SELECT
  toUInt64(assumeNotNull(series_id)) AS series_id,
  cast(source AS LowCardinality(String)) AS source,
  toUInt64(assumeNotNull(asset_id)) AS asset_id,
  cast(asset_kind AS Enum8('oracle' = 1, 'share' = 2)) AS asset_kind,
  cast(obs_kind AS Enum8('usd' = 1, 'ppfs' = 2)) AS obs_kind,
  toUInt8(priority) AS priority,
  toUInt64(ifNull(want_asset_id, 0)) AS want_asset_id,
  toFloat64(ppfs_log10) AS ppfs_log10,
  toFloat64(scale) AS scale,
  scale_until,
  valid_from,
  valid_to
FROM rows
-- A series key with a missing part (no symbol, no chain key) names no series.
WHERE series_id IS NOT NULL AND asset_id IS NOT NULL
