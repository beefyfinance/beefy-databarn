{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['marts', 'tvl', 'stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
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

SELECT
  hs.chain_id,
  p.product_type,
  p.beefy_key,
  hs.product_address,
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
FROM {{ ref('int_product_stats__unified_hourly') }} hs FINAL
INNER JOIN {{ ref('product') }} p
  ON hs.chain_id = p.chain_id
  AND hs.product_address = p.product_address
{% if is_incremental() %}
  WHERE hs.date_hour >= toDateTime('{{ max_date }}') - INTERVAL 1 DAY
{% endif %}