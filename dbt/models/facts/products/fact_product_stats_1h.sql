-- depends_on: {{ ref('int_price_obs') }}
-- depends_on: {{ ref('int_product_price_assets') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['tvl', 'stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
    post_hook="{{ price_consumer_done('fact_product_stats_1h') }}",
  )
}}

{% if is_incremental() %}
  {% set max_date_sql %}
    select max(cast(date_hour as Nullable(DateTime('UTC')))) as max_date_hour
    from {{ this }}
  {% endset %}
  {% set max_date_tbl = run_query(max_date_sql) %}
  {% set max_date = max_date_tbl.columns[0][0] %}
  {% if max_date is none %}
    {% set max_date = '2021-07-31 19:30:00' %}
  {% endif %}
{% else %}
  {% set max_date = '2021-07-31 19:30:00' %}
{% endif %}
{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}
{% if is_incremental() %}
  {#- Hours a resolver run re-priced for a product (price_reprice); unified_1h re-emitted them in this run. -#}
  {% set rp = price_reprice('fact_product_stats_1h', run_query("SELECT toString(toDateTime('" ~ max_date ~ "', 'UTC') - INTERVAL 1 DAY)").rows[0][0] if execute else none, 'product', 'hour') %}
{% endif %}

SELECT
  hs.chain_id as chain_id,
  c.chain_name as chain_name,
  p.product_type,
  p.beefy_key as beefy_key,
  hs.product_address as product_address,
  p.display_name,
  p.is_active,
  p.platform_id,
  hs.date_hour,
  hs.tvl_usd,
  hs.apy,
  hs.compoundings_per_year,
  hs.beefy_performance_fee,
  hs.lp_fee,
  hs.total_apy,
  hs.vault_apr,
  hs.trading_apr,
  hs.clm_apr,
  hs.reward_pool_apr,
  hs.reward_pool_trading_apr,
  hs.vault_apy,
  hs.liquid_staking_apr,
  hs.composable_pool_apr,
  hs.merkl_apr,
  hs.linea_ignition_apr,
  hs.lp_price,
  hs.breakdown_tokens,
  hs.breakdown_balances,
  hs.total_supply,
  hs.underlying_liquidity,
  hs.underlying_balances,
  hs.underlying_price,
  hs.underlying_amount_compounded,
  hs.underlying_token_price_usd,
  hs.underlying_amount_compounded_usd,
  hs.harvest_call_fee,
  hs.harvest_call_fee_usd,
  hs.gas_fee,
  hs.gas_fee_usd,
  hs.platform_fee,
  hs.platform_fee_usd,
  hs.strategist_fee,
  hs.strategist_fee_usd,
  hs.harvest_amount,
  hs.harvest_amount_usd,
  hs.avg_native_price,
  hs.harvest_txn_count,
  hs.harvest_vault_count,
  hs.share_token_price_usd
FROM {{ ref('int_product_stats__unified_1h') }} hs FINAL
INNER JOIN {{ ref('dim_chain') }} c
  ON hs.chain_id = c.chain_id
INNER JOIN {{ ref('dim_product') }} p
  ON hs.chain_id = p.chain_id
  AND hs.product_address = p.product_address
{% if is_incremental() %}
  WHERE (
  {% if since is not none %}
  hs.date_hour >= toDateTime('{{ since }}')
  {% else %}
  hs.date_hour >= toDateTime('{{ max_date }}') - INTERVAL 1 DAY
  OR {{ price_repriced(rp, price_product_key('hs.chain_id', 'hs.product_address'), 'hs.date_hour') }}
  {% endif %}
     -- Re-load keys that already violate uniqueness in the test window.
     -- delete+insert only covers the 1-day lookback, so a one-off join
     -- fanout leaves extras in MergeTree until they are selected again.
     OR (hs.date_hour, hs.chain_id, hs.product_address) IN (
       SELECT date_hour, chain_id, product_address
       FROM {{ this }}
       WHERE date_hour >= toStartOfHour(now('UTC')) - INTERVAL 30 DAY
       GROUP BY date_hour, chain_id, product_address
       HAVING count() > 1
     )
  )
  {% if until is not none %}
  -- Batched reimport (meta.batch): cap the window so one run never reads all of unified_1h.
  AND hs.date_hour < toDateTime('{{ until }}')
  {% endif %}
{% elif since is not none and until is not none %}
-- First batch of a batched full refresh: only that window.
WHERE hs.date_hour >= toDateTime('{{ since }}') AND hs.date_hour < toDateTime('{{ until }}')
{% endif %}
-- Grain is (chain_id, product_address, date_hour). CoalescingMergeTree FINAL
-- should already be unique; keep one row if a join ever fans out. The fanned-out rows share every hs column, so
-- no ORDER BY: sorting a month of wide rows before LIMIT BY took 3.9 GB, LIMIT BY alone streams.
LIMIT 1 BY hs.chain_id, hs.product_address, hs.date_hour
