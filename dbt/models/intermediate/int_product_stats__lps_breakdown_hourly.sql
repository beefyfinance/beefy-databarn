{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}

-- lp_price is the API /lps/breakdown snapshot price of the vault's LP oracle (vault_id is the oracle key).
-- Inside a known-bad window it is replaced by int_price_oracle_corrections on the snapshot's 5-minute slot.
-- Corrections are a tiny table: exact-key left join. join_use_nulls=0, so a miss has price_source = ''.
-- Other breakdown columns (underlying_price, balances) are not corrected.
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
WHERE
  price between 0 and 1000000
  and total_supply between 0 and 1000000000000
  and underlying_liquidity between 0 and 1000000000000
  and underlying_price between 0 and 1000000
GROUP BY chain_id, product_address, toStartOfHour(date_time)
