{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

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
  {% set since = incremental_since_var() %}
  {% if since is not none %}
    {% set threshold = since %}
  {% endif %}
{% endif %}

-- Incremental on date_hour (1-day lookback), like int_product_stats__tvl_hourly.
SELECT
  p.chain_id,
  p.product_address,
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
WHERE 
  ab.compoundings_per_year between 0 and 1000000000
  and ab.beefy_performance_fee between 0 and 1000000000
  and ab.lp_fee between 0 and 1000000000
  and ab.total_apy between 0 and 1000000
  and ab.vault_apr between 0 and 1000000
  and ab.trading_apr between 0 and 1000000
  and ab.clm_apr between 0 and 1000000
  and ab.reward_pool_apr between 0 and 1000000
  and ab.reward_pool_trading_apr between 0 and 1000000
  and ab.vault_apy between 0 and 1000000
  {% if is_incremental() %}
  and ab.date_time >= toDateTime('{{ threshold }}')
  and ab.date_time < now() + INTERVAL 1 DAY
  {% endif %}
GROUP BY p.chain_id, p.product_address, toStartOfHour(ab.date_time)