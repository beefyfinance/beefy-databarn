{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}

SELECT
  p.chain_id,
  p.product_address,
  toStartOfHour(lb.date_time) as date_hour,
  argMax(lb.price, lb.date_time) as lp_price,
  argMax(lb.tokens, lb.date_time) as breakdown_tokens,
  argMax(lb.balances, lb.date_time) as breakdown_balances,
  argMax(lb.total_supply, lb.date_time) as total_supply,
  argMax(lb.underlying_liquidity, lb.date_time) as underlying_liquidity,
  argMax(lb.underlying_balances, lb.date_time) as underlying_balances,
  argMax(lb.underlying_price, lb.date_time) as underlying_price
FROM {{ ref('stg_beefy_api__lps_breakdown') }} lb
INNER JOIN {{ ref('product') }} p
  ON lb.vault_id = p.beefy_key
WHERE 
  lb.price between 0 and 1000000
  and lb.total_supply between 0 and 1000000000000
  and lb.underlying_liquidity between 0 and 1000000000000
  and lb.underlying_price between 0 and 1000000
GROUP BY p.chain_id, p.product_address, toStartOfHour(lb.date_time)
