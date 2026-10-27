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

-- lp_price is the API /lps/breakdown snapshot price of the vault's LP oracle (vault_id is the oracle key).
-- Inside a known-bad window it is replaced by int_price_oracle_corrections on the snapshot's 5-minute slot.
-- Corrections are a tiny table: exact-key left join. join_use_nulls=0, so a miss has price_source = ''.
-- Other breakdown columns (underlying_price, balances) are not corrected.
-- Incremental on date_hour (1-day lookback), plus every hour touching a correction so new windows
-- reach old hours. Removing a window needs a reimport.
WITH breakdown AS (
  SELECT
    p.chain_id as chain_id,
    p.product_address as product_address,
    lb.date_time as date_time,
    if(corr.price_source != '', toFloat64(corr.price), lb.price) as price,
    lb.tokens as tokens,
    lb.balances as balances,
    lb.total_supply as total_supply,
    lb.underlying_liquidity as underlying_liquidity,
    lb.underlying_balances as underlying_balances,
    lb.underlying_price as underlying_price
  FROM {{ ref('stg_beefy_api__lps_breakdown') }} lb
  INNER JOIN {{ ref('int_product_keys') }} p
    ON lb.vault_id = p.beefy_key
  LEFT JOIN {{ ref('int_price_oracle_corrections') }} corr
    ON lb.vault_id = corr.beefy_price_oracle_key
    AND toStartOfFiveMinutes(lb.date_time) = corr.date_time
  -- Filter on raw columns before aggregating: the outer aliases shadow these names.
  WHERE
    if(corr.price_source != '', toFloat64(corr.price), lb.price) between 0 and 1000000
    and lb.total_supply between 0 and 1000000000000
    and lb.underlying_liquidity between 0 and 1000000000000
    and lb.underlying_price between 0 and 1000000
    {% if is_incremental() %}
    and (
      (lb.date_time >= toDateTime('{{ threshold }}') and lb.date_time < now() + INTERVAL 1 DAY)
      or (lb.vault_id, toStartOfHour(lb.date_time)) in (
        select beefy_price_oracle_key, toStartOfHour(date_time) from {{ ref('int_price_oracle_corrections') }}
      )
    )
    {% endif %}
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(price, date_time) as lp_price,
  argMax(tokens, date_time) as breakdown_tokens,
  argMax(balances, date_time) as breakdown_balances,
  argMax(total_supply, date_time) as total_supply,
  argMax(underlying_liquidity, date_time) as underlying_liquidity,
  argMax(underlying_balances, date_time) as underlying_balances,
  argMax(underlying_price, date_time) as underlying_price
FROM breakdown
GROUP BY chain_id, product_address, toStartOfHour(date_time)
