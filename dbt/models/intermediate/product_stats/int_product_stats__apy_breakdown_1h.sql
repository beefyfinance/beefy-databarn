{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'fact_product_stats_1h'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
  )
}}

{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}
{% if is_incremental() %}
  {% set threshold_sql %}
    SELECT max(date_hour) - INTERVAL 1 DAY FROM {{ this }}
  {% endset %}
  {% set threshold_result = run_query(threshold_sql) %}
  {% if threshold_result and threshold_result.rows | length > 0 and threshold_result.rows[0][0] %}
    {% set threshold = threshold_result.rows[0][0] | string %}
  {% else %}
    {% set threshold = '1900-01-01 00:00:00' %}
  {% endif %}
  {% if since is not none %}
    {% set threshold = since %}
  {% endif %}
{% elif since is not none %}
  {#- First batch of a batched full refresh: only that window. -#}
  {% set threshold = since %}
{% endif %}

-- Incremental on date_hour (1-day lookback), like int_product_stats__tvl_1h.
SELECT
  p.chain_id,
  toLowCardinality(p.product_address) AS product_address,
  toStartOfHour(ab.date_time) as date_hour,
  argMax(ab.compoundings_per_year, ab.date_time) as compoundings_per_year,
  argMax(ab.beefy_performance_fee, ab.date_time) as beefy_performance_fee,
  argMax(ab.lp_fee, ab.date_time) as lp_fee,
  argMax(ab.total_apy, ab.date_time) as total_apy,
  argMax(ab.vault_apr, ab.date_time) as vault_apr,
  argMax(ab.trading_apr, ab.date_time) as trading_apr,
  argMax(ab.clm_apr, ab.date_time) as clm_apr,
  argMax(ab.reward_pool_apr, ab.date_time) as reward_pool_apr,
  argMax(ab.reward_pool_trading_apr, ab.date_time) as reward_pool_trading_apr,
  argMax(ab.vault_apy, ab.date_time) as vault_apy,
  argMax(ab.liquid_staking_apr, ab.date_time) as liquid_staking_apr,
  argMax(ab.composable_pool_apr, ab.date_time) as composable_pool_apr,
  argMax(ab.merkl_apr, ab.date_time) as merkl_apr,
  argMax(ab.linea_ignition_apr, ab.date_time) as linea_ignition_apr
FROM {{ ref('stg_beefy_api__apy_breakdown') }} ab
INNER JOIN {{ ref('int_product_keys') }} p
  ON ab.vault_id = p.beefy_key
-- The API omits the fields that do not apply to a product (clm_apr on a classic vault, vault_apy on a CLM...), so
-- every column but date_time can be NULL: a NULL passes, only a present out-of-range value drops the row.
WHERE
  ifNull(ab.compoundings_per_year, 0) between 0 and 1000000000
  and ifNull(ab.beefy_performance_fee, 0) between 0 and 1000000000
  and ifNull(ab.lp_fee, 0) between 0 and 1000000000
  and ifNull(ab.total_apy, 0) between 0 and 1000000
  and ifNull(ab.vault_apr, 0) between 0 and 1000000
  and ifNull(ab.trading_apr, 0) between 0 and 1000000
  and ifNull(ab.clm_apr, 0) between 0 and 1000000
  and ifNull(ab.reward_pool_apr, 0) between 0 and 1000000
  and ifNull(ab.reward_pool_trading_apr, 0) between 0 and 1000000
  and ifNull(ab.vault_apy, 0) between 0 and 1000000
  and ifNull(ab.liquid_staking_apr, 0) between 0 and 1000000
  and ifNull(ab.composable_pool_apr, 0) between 0 and 1000000
  and ifNull(ab.merkl_apr, 0) between 0 and 1000000
  and ifNull(ab.linea_ignition_apr, 0) between 0 and 1000000
  {% if threshold is defined %}
  and ab.date_time >= toDateTime('{{ threshold }}')
  and ab.date_time < now() + INTERVAL 1 DAY
  {% endif %}
  {% if until is not none %}
  -- Batched reimport (meta.batch): one month per run.
  and ab.date_time < toDateTime('{{ until }}')
  {% endif %}
GROUP BY p.chain_id, p.product_address, toStartOfHour(ab.date_time)
