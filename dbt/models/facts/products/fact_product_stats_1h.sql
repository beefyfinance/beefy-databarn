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

{#- Value columns, in table order: (name, expression). hs is int_product_stats__unified_1h FINAL (internal names),
    lb is int_product_stats__lps_breakdown_1h (the arrays: unified cannot keep them). Each value is cast to its
    contract type (yml), so a stored row and its recomputation hash the same (diff below). -#}
{% set value_columns = [
  ('tvl_usd', 'hs.tvl_usd'),
  ('apy', 'coalesce(hs.total_apy, hs.apy)'),
  ('share_price_usd', 'hs.share_token_price_usd'),
  ('want_price_usd', 'coalesce(hs.lp_price, hs.want_price_usd)'),

  ('apy_vault', 'hs.vault_apy'),
  ('apr_vault', 'hs.vault_apr'),
  ('apr_trading', 'hs.trading_apr'),
  ('apr_clm', 'hs.clm_apr'),
  ('apr_reward_pool', 'hs.reward_pool_apr'),
  ('apr_reward_pool_trading', 'hs.reward_pool_trading_apr'),
  ('apr_liquid_staking', 'hs.liquid_staking_apr'),
  ('apr_composable_pool', 'hs.composable_pool_apr'),
  ('apr_merkl', 'hs.merkl_apr'),
  ('apr_linea_ignition', 'hs.linea_ignition_apr'),
  ('fee_rate_performance', 'hs.beefy_performance_fee'),
  ('fee_rate_lp', 'hs.lp_fee'),
  ('compoundings_per_year', 'hs.compoundings_per_year'),

  ('want_total_supply', 'hs.total_supply'),
  ('want_tokens', 'lb.breakdown_tokens'),
  ('want_balances', 'lb.breakdown_balances'),

  ('clm_pool_liquidity', 'hs.underlying_liquidity'),
  ('clm_pool_liquidity_price_usd', 'hs.underlying_price'),
  ('clm_pool_balances', 'lb.underlying_balances'),

  ('harvest_count', 'hs.harvest_txn_count'),
  ('harvest_amount', 'hs.harvest_amount'),
  ('harvest_amount_usd', 'hs.harvest_amount_usd'),
  ('harvest_call_fee', 'hs.harvest_call_fee'),
  ('harvest_call_fee_usd', 'hs.harvest_call_fee_usd'),
  ('harvest_gas_fee', 'hs.gas_fee'),
  ('harvest_gas_fee_usd', 'hs.gas_fee_usd'),
  ('harvest_platform_fee', 'hs.platform_fee'),
  ('harvest_platform_fee_usd', 'hs.platform_fee_usd'),
  ('harvest_strategist_fee', 'hs.strategist_fee'),
  ('harvest_strategist_fee_usd', 'hs.strategist_fee_usd'),
  ('harvest_native_price_usd', 'hs.avg_native_price'),
] %}

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
{#- Days of unified compared with the fact each run: the longest source lookback of int_product_stats__unified_1h. -#}
{% set diff_days = 30 %}
{% if is_incremental() %}
  {#- Hours a resolver run re-priced for a product (price_reprice); unified_1h re-emitted them in this run. -#}
  {% set rp = price_reprice('fact_product_stats_1h', run_query("SELECT toString(toDateTime('" ~ max_date ~ "', 'UTC') - INTERVAL 1 DAY)").rows[0][0] if execute else none, 'product', 'hour') %}
{% endif %}

{#- Lower bound of the lps_breakdown_1h rows the arrays are joined from: every branch of the window below lies after
    it (the diff and duplicate checks look back diff_days, price_reprice at most price_reprice_max_days). -#}
{% if since is not none %}
  {% set lb_where = "date_hour >= toDateTime('" ~ since ~ "')" ~ ((" AND date_hour < toDateTime('" ~ until ~ "')") if until is not none else '') %}
{% elif is_incremental() %}
  {% set lb_where = "date_hour >= least(toDateTime('" ~ max_date ~ "', 'UTC') - INTERVAL 1 DAY, toStartOfHour(now('UTC')) - INTERVAL " ~ ([diff_days, var('price_reprice_max_days', 31) | int] | max) ~ " DAY)" %}
{% else %}
  {% set lb_where = '1' %}
{% endif %}

SELECT
  hs.chain_id as chain_id,
  toLowCardinality(c.chain_name) as chain_name,
  toLowCardinality(p.product_type) as product_type,
  toLowCardinality(p.beefy_key) as beefy_key,
  toLowCardinality(hs.product_address) as product_address,
  toLowCardinality(p.display_name) as display_name,
  p.is_active,
  toLowCardinality(p.platform_id) as platform_id,
  hs.date_hour as date_hour,
  {% for name, expr in value_columns -%}
  {{ contract_cast(name, expr) }} as {{ name }}{% if not loop.last %},{% endif %}
  {% endfor %}
FROM {{ ref('int_product_stats__unified_1h') }} hs FINAL
INNER JOIN {{ ref('dim_chain') }} c
  ON hs.chain_id = c.chain_id
INNER JOIN {{ ref('dim_product') }} p
  ON hs.chain_id = p.chain_id
  AND hs.product_address = p.product_address
-- join_use_nulls = 0: an hour without a breakdown snapshot gets empty arrays.
LEFT JOIN (
  SELECT chain_id, product_address, date_hour, breakdown_tokens, breakdown_balances, underlying_balances
  FROM {{ ref('int_product_stats__lps_breakdown_1h') }}
  WHERE {{ lb_where }}
) lb
  ON hs.chain_id = lb.chain_id
  AND hs.product_address = lb.product_address
  AND hs.date_hour = lb.date_hour
{% if is_incremental() %}
  WHERE (
  {% if since is not none %}
  hs.date_hour >= toDateTime('{{ since }}')
  {% else %}
  hs.date_hour >= toDateTime('{{ max_date }}') - INTERVAL 1 DAY
  OR {{ price_repriced(rp, price_product_key('hs.chain_id', 'hs.product_address'), 'hs.date_hour') }}
     -- Late values: the hours of the last diff_days where unified no longer matches the fact. Unified re-reads up to
     -- 30 days of some sources (a new product's history, a source that was skipped or reloaded), which a 1-day
     -- window never picked up (apy was missing from the fact for years). Only the rows that differ are rewritten,
     -- so the per-run purge of deleted rows stays small. Rows compare by sipHash128 of their values, cast to the
     -- contract types on both sides (it tells NULL from 0; 18 s over 30 days locally, 28 s with toString). The
     -- arrays (lps_breakdown_1h) are not compared: that model only rewrites old hours through price_reprice or a
     -- reimport, both handled above.
     OR (hs.date_hour, hs.chain_id, hs.product_address) IN (
       SELECT date_hour, chain_id, product_address
       FROM (
         SELECT hs.date_hour AS date_hour, hs.chain_id AS chain_id, hs.product_address AS product_address,
           sipHash128(tuple(
             {%- for name, expr in value_columns if not expr.startswith('lb.') %}
             {{ contract_cast(name, expr) }}{% if not loop.last %},{% endif %}
             {%- endfor %}
           )) AS v
         FROM {{ ref('int_product_stats__unified_1h') }} hs FINAL
         WHERE hs.date_hour >= toStartOfHour(now('UTC')) - INTERVAL {{ diff_days }} DAY
       ) u
       LEFT ANTI JOIN (
         SELECT date_hour, chain_id, product_address,
           sipHash128(tuple(
             {%- for name, expr in value_columns if not expr.startswith('lb.') %}
             {{ name }}{% if not loop.last %},{% endif %}
             {%- endfor %}
           )) AS v
         FROM {{ this }}
         WHERE date_hour >= toStartOfHour(now('UTC')) - INTERVAL {{ diff_days }} DAY
       ) f USING (date_hour, chain_id, product_address, v)
     )
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
