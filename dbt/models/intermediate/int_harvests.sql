-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'harvest'],
    unique_key=['network_id', 'vault_beefy_key', 'txn_timestamp', 'txn_hash', 'event_idx'],
    order_by=['txn_timestamp', 'network_id', 'block_number', 'txn_idx', 'event_idx'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
    post_hook="{{ price_consumer_done('int_harvests') }}",
  )
}}

{% if is_incremental() %}
  {% set threshold = run_query("SELECT toString(max(txn_timestamp) - INTERVAL 1 DAY) FROM " ~ this).rows[0][0] %}
  {% set since = incremental_since_var() %}
  {% if since is not none %}
    {% set threshold = since %}
  {% endif %}
  {% set rp = price_reprice('int_harvests', threshold, 'asset') %}
{% endif %}

-- Harvests priced with asset_price_5m on the harvest's 5-minute slot, like every priced model:
--   want_price         the product's want oracle (beefy_wants_price_oracle_key)
--   native_price       the chain's native oracle (beefy_native_price_oracle_key)
--   platform_fee_price native, except Fraxtal (252) where platform fees are paid in frxETH
-- Corrections are part of the series. A harvest whose series has no price keeps the price beefy-db stored with it
-- (price source 'harvest'). The stored prices are not price sources: the want price is often wrong (2026-11: 6.7%
-- of harvests more than 50% off the beefy_db price, many a placeholder 1 on CLM wants).
-- Incremental on txn_timestamp (1-day lookback), plus the harvests a resolver run re-priced (price_reprice: a dirty
-- want, native or fee asset from its first changed slot). Pricing every harvest reads most of asset_price_5m
-- (15M harvests: 2.6B rows, 50 s, 1.3 GiB), too much for every run. A harvest that a re-priced want pushes over the
-- $30M cap keeps its old row until a reimport (make dbt reimport int_harvests+ <since>).

WITH
  oracles AS (
    SELECT oracle_id, {{ asset_id_from_beefy_price_oracle_id('id') }} AS asset_id
    FROM {{ ref('stg_beefy_db__price_oracles') }}
  ),
  harvests_keyed AS (
    SELECT
      h.*,
      toStartOfFiveMinutes(h.txn_timestamp) AS slot,
      w.asset_id AS want_asset_id,
      n.asset_id AS native_asset_id,
      if(h.network_id = 252, (SELECT any(asset_id) FROM oracles WHERE oracle_id = 'frxETH'), n.asset_id) AS fee_asset_id
    FROM {{ ref('stg_beefy_db__harvests') }} h
    LEFT JOIN {{ ref('int_product_keys') }} pk
      ON h.network_id = pk.chain_id
      AND h.vault_beefy_key = pk.beefy_key
    LEFT JOIN {{ ref('int_chain_keys') }} c
      ON h.network_id = c.chain_id
    LEFT JOIN oracles w ON pk.beefy_wants_price_oracle_key = w.oracle_id
    LEFT JOIN oracles n ON c.beefy_native_price_oracle_key = n.oracle_id
  ),
  harvests AS (
    SELECT * FROM harvests_keyed
    {% if is_incremental() %}
    WHERE (txn_timestamp >= toDateTime('{{ threshold }}', 'UTC') AND txn_timestamp < now() + INTERVAL 1 DAY)
      OR {{ price_repriced(rp, 'ifNull(want_asset_id, 0)', 'txn_timestamp') }}
      OR {{ price_repriced(rp, 'ifNull(native_asset_id, 0)', 'txn_timestamp') }}
      OR {{ price_repriced(rp, 'ifNull(fee_asset_id, 0)', 'txn_timestamp') }}
    {% endif %}
  ),
  -- One key lookup per price: the set of (asset, slot) pairs prunes asset_price_5m on its primary key.
  want_prices AS (
    SELECT asset_id, date_time, price, price_source FROM {{ ref('asset_price_5m') }}
    WHERE (asset_id, date_time) IN (SELECT want_asset_id, slot FROM harvests)
  ),
  native_prices AS (
    SELECT asset_id, date_time, price, price_source FROM {{ ref('asset_price_5m') }}
    WHERE (asset_id, date_time) IN (SELECT native_asset_id, slot FROM harvests)
  ),
  fee_prices AS (
    SELECT asset_id, date_time, price FROM {{ ref('asset_price_5m') }}
    WHERE (asset_id, date_time) IN (SELECT fee_asset_id, slot FROM harvests WHERE network_id = 252)
  ),
  priced AS (
    SELECT
      h.network_id AS network_id,
      h.block_number AS block_number,
      h.txn_idx AS txn_idx,
      h.event_idx AS event_idx,
      h.txn_timestamp AS txn_timestamp,
      h.txn_hash AS txn_hash,
      h.vault_beefy_key AS vault_beefy_key,
      h.call_fee AS call_fee,
      h.gas_fee AS gas_fee,
      h.raw_gas_fee AS raw_gas_fee,
      h.platform_fee AS platform_fee,
      h.strategist_fee AS strategist_fee,
      h.harvest_amount AS harvest_amount,
      h.is_cowllector AS is_cowllector,
      h.strategist_address AS strategist_address,
      h.want_asset_id AS want_asset_id,
      h.native_asset_id AS native_asset_id,
      -- join_use_nulls = 0: a miss has price 0 and price_source ''.
      toDecimal256(if(wp.price > 0, wp.price, toFloat64(h.want_price)), 20) AS want_price,
      toDecimal256(if(np.price > 0, np.price, toFloat64(h.native_price)), 20) AS native_price,
      toDecimal256(multiIf(fp.price > 0, fp.price, np.price > 0, np.price, toFloat64(h.native_price)), 20) AS platform_fee_price,
      cast(if(wp.price > 0, wp.price_source, 'harvest') AS LowCardinality(String)) AS want_price_source,
      cast(if(np.price > 0, np.price_source, 'harvest') AS LowCardinality(String)) AS native_price_source
    FROM harvests h
    LEFT JOIN want_prices wp ON wp.asset_id = h.want_asset_id AND wp.date_time = h.slot
    LEFT JOIN native_prices np ON np.asset_id = h.native_asset_id AND np.date_time = h.slot
    LEFT JOIN fee_prices fp ON fp.asset_id = h.fee_asset_id AND fp.date_time = h.slot
  )

SELECT
  *,
  toBool(startsWith(CAST(want_price_source AS String), 'corr_')) AS is_want_price_corrected,
  toBool(startsWith(CAST(native_price_source AS String), 'corr_')) AS is_native_price_corrected
FROM priced
-- price is off for some harvests in the feed
-- biggest real harvest we saw was this ($9M)
-- https://etherscan.io/tx/0x31b8083e467ed217523655f9b26b71f154fd1358e633b275011123c268a88901
-- next biggest harvest is bugged and shows as $32M
WHERE toDecimal256(harvest_amount * want_price, 20) < 30_000_000.00
