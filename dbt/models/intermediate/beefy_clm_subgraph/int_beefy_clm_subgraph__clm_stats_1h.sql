{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'fact_product_stats_1h'],
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

-- Hourly CLM stats from the subgraph. Fallback for fact_product_stats_1h columns.
-- Token prices are integers in the native token's decimals, so a cheap token can truncate to 0 (or to one
-- digit) on a 6-decimal native chain. USD values therefore price both balances in one token, the better
-- priced one, through the exact pool price, instead of multiplying each balance by its own price.
-- native_to_usd_price is scaled so that to_native * native_to_usd / 1e36 is USD on every chain.
-- After a beefy_clm_subgraph backfill: make dbt reimport int_beefy_clm_subgraph__clm_stats_1h+ <since>.

with snapshots as (
  select
    pk.chain_id as chain_id,
    pk.product_address as product_address,
    s.period_start_at as date_hour,
    {{ to_representation_evm_address('c.underlying_token0_address') }} as token0_representation_address,
    {{ to_representation_evm_address('c.underlying_token1_address') }} as token1_representation_address,
    toFloat64(assumeNotNull(s.total_underlying_amount0)) / pow(10, t0.decimals) as balance0,
    toFloat64(assumeNotNull(s.total_underlying_amount1)) / pow(10, t1.decimals) as balance1,
    toFloat64(assumeNotNull(s.manager_total_supply)) / 1e18 as manager_total_supply,
    toFloat64(assumeNotNull(s.price_of_token0_in_token1)) / pow(10, t1.decimals) as token1_per_token0,
    toFloat64(assumeNotNull(s.token0_to_native_price)) * toFloat64(assumeNotNull(s.native_to_usd_price)) / 1e36 as token0_price_usd,
    toFloat64(assumeNotNull(s.token1_to_native_price)) * toFloat64(assumeNotNull(s.native_to_usd_price)) / 1e36 as token1_price_usd
  from {{ ref('stg_beefy_clm_subgraph__clm_snapshots') }} s
  inner join {{ ref('stg_beefy_clm_subgraph__clms') }} c
    on s.chain_key = c.chain_key
    and s.clm_address = c.clm_address
  inner join {{ ref('stg_beefy_clm_subgraph__tokens') }} t0
    on c.chain_key = t0.chain_key
    and c.underlying_token0_address = t0.token_address
  inner join {{ ref('stg_beefy_clm_subgraph__tokens') }} t1
    on c.chain_key = t1.chain_key
    and c.underlying_token1_address = t1.token_address
  inner join {{ ref('dim_chain') }} chain_dim
    on s.chain_key = chain_dim.beefy_key
  inner join {{ ref('int_product_keys') }} pk
    on chain_dim.chain_id = pk.chain_id
    and {{ evm_address('s.clm_address') }} = pk.product_address
  where s.period_seconds = 3600
    and s.manager_total_supply >= 0
    and s.total_underlying_amount0 >= 0
    and s.total_underlying_amount1 >= 0
    {% if is_incremental() %}
    and s.period_start_at >= toDateTime('{{ threshold }}')
    {% endif %}
),

priced as (
  select
    *,
    if(
      token1_per_token0 > 0 and greatest(token0_price_usd, token1_price_usd) > 0,
      toNullable(if(
        token0_price_usd >= token1_price_usd,
        (balance0 + balance1 / token1_per_token0) * token0_price_usd,
        (balance0 * token1_per_token0 + balance1) * token1_price_usd
      )),
      null
    ) as tvl_usd
  from snapshots
)

select
  chain_id,
  product_address,
  date_hour,
  token0_representation_address,
  token1_representation_address,
  balance0,
  balance1,
  manager_total_supply,
  tvl_usd,
  if(manager_total_supply > 0, tvl_usd / manager_total_supply, null) as share_token_price_usd
from priced
