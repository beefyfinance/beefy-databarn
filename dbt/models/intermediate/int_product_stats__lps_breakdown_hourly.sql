-- depends_on: {{ ref('int_price_obs') }}
-- depends_on: {{ ref('int_product_price_assets') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'product_stats'],
    unique_key=['chain_id', 'product_address', 'date_hour'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
    post_hook="{{ price_consumer_done('int_product_stats__lps_breakdown_hourly') }}",
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
  {% set rp = price_reprice('int_product_stats__lps_breakdown_hourly', threshold, 'product', 'hour') %}
{% endif %}

{#- Rows of an hour to (re)compute: the incremental window, plus the hours a resolver run re-priced for a product
    (price_reprice: from the hour of the first changed price of any asset pricing it). All sources of those hours.
    One filter per source: window[name] filters its columns. -#}
{% set window = {} %}
{% for name, c, a, h in [('api', 'p.chain_id', 'p.product_address', 'toStartOfHour(lb.date_time)'), ('clm', 'c.chain_id', 'c.product_address', 'c.date_hour'), ('dbv1', 'd.chain_id', 'd.product_address', 'd.date_hour')] %}
  {% set upper = (' and ' ~ h ~ " < toDateTime('" ~ until ~ "')") if until is not none else '' %}
  {% if is_incremental() %}
    {% do window.update({name: '((' ~ h ~ " >= toDateTime('" ~ threshold ~ "') and " ~ h ~ ' < now() + INTERVAL 1 DAY) or '
        ~ price_repriced(rp, price_product_key(c, a), h) ~ ')' ~ upper}) %}
  {% elif since is not none %}
    {#- First batch of a batched full refresh: only that window. -#}
    {% do window.update({name: '(' ~ h ~ " >= toDateTime('" ~ since ~ "')" ~ upper ~ ')'}) %}
  {% else %}
    {% do window.update({name: '1'}) %}
  {% endif %}
{% endfor %}

-- lp_price is the asset_price_5m price of the vault's LP oracle (the beefy_db oracle keyed by the vault id) on the
-- 5-minute slot of the hour's last /lps/breakdown snapshot: the snapshot price is that oracle's price, corrected
-- inside known-bad windows. A vault with no such oracle keeps the snapshot price. Other breakdown columns
-- (underlying_price, balances) are the snapshot's.
-- API rows win over fallbacks for history (priority 0): CLM subgraph share price, supply and token balances,
-- databarn v1 want price and want supply for classic vaults.
-- Classic vaults have no underlying_liquidity or underlying_price, hence the ifNull in the range filters.
-- Incremental on date_hour (1-day lookback), plus the hours re-priced by the resolver (window).
WITH snapshots AS (
  SELECT
    p.chain_id as chain_id,
    p.product_address as product_address,
    toStartOfHour(lb.date_time) as date_hour,
    max(lb.date_time) as date_time,
    argMax(lb.price, lb.date_time) as snapshot_price,
    argMax(lb.tokens, lb.date_time) as tokens,
    argMax(lb.balances, lb.date_time) as balances,
    argMax(lb.total_supply, lb.date_time) as total_supply,
    argMax(lb.underlying_liquidity, lb.date_time) as underlying_liquidity,
    argMax(lb.underlying_balances, lb.date_time) as underlying_balances,
    argMax(lb.underlying_price, lb.date_time) as underlying_price,
    any(lb.vault_id) as vault_id
  FROM {{ ref('stg_beefy_api__lps_breakdown') }} lb
  INNER JOIN {{ ref('int_product_keys') }} p
    ON lb.vault_id = p.beefy_key
  -- Filter on raw columns before aggregating: the aliases shadow these names.
  WHERE
    lb.price between 0 and 1000000
    and lb.total_supply between 0 and 1000000000000
    and ifNull(lb.underlying_liquidity, 0) between 0 and 1000000000000
    and ifNull(lb.underlying_price, 0) between 0 and 1000000
    and {{ window['api'] }}
  GROUP BY p.chain_id, p.product_address, date_hour
),
-- The LP oracle of each vault. join_use_nulls = 0: a vault with no oracle gets the id of no asset, so no price.
lp_oracles AS (
  SELECT oracle_id AS vault_id, {{ asset_id_from_beefy_price_oracle_id('id') }} AS lp_asset_id
  FROM {{ ref('stg_beefy_db__price_oracles') }}
),
-- Key set of the price lookup, from scalar columns only: snapshots aggregates every array column, and running it
-- a second time for the key set ran out of memory on a month batch.
lp_keys AS (
  SELECT o.lp_asset_id AS asset_id, toStartOfFiveMinutes(k.date_time) AS slot
  FROM (
    SELECT lb.vault_id AS vault_id, max(lb.date_time) AS date_time
    FROM {{ ref('stg_beefy_api__lps_breakdown') }} lb
    INNER JOIN {{ ref('int_product_keys') }} p
      ON lb.vault_id = p.beefy_key
    WHERE
      lb.price between 0 and 1000000
      and lb.total_supply between 0 and 1000000000000
      and ifNull(lb.underlying_liquidity, 0) between 0 and 1000000000000
      and ifNull(lb.underlying_price, 0) between 0 and 1000000
      and {{ window['api'] }}
    GROUP BY lb.vault_id, toStartOfHour(lb.date_time)
  ) k
  INNER JOIN lp_oracles o ON o.vault_id = k.vault_id
),
lp_prices AS (
  SELECT asset_id, date_time, price FROM {{ ref('asset_price_5m') }}
  WHERE (asset_id, date_time) IN (SELECT asset_id, slot FROM lp_keys)
),
breakdown AS (
  SELECT
    s.chain_id as chain_id,
    s.product_address as product_address,
    s.date_time as date_time,
    -- join_use_nulls = 0: a miss has price 0.
    if(lp.price > 0 and lp.price <= 1000000, lp.price, s.snapshot_price) as price,
    s.tokens as tokens,
    s.balances as balances,
    s.total_supply as total_supply,
    s.underlying_liquidity as underlying_liquidity,
    s.underlying_balances as underlying_balances,
    s.underlying_price as underlying_price,
    1 as priority
  FROM snapshots s
  LEFT JOIN lp_oracles o ON o.vault_id = s.vault_id
  LEFT JOIN lp_prices lp
    ON lp.asset_id = o.lp_asset_id
    AND lp.date_time = toStartOfFiveMinutes(s.date_time)

  UNION ALL

  SELECT
    c.chain_id as chain_id,
    c.product_address as product_address,
    c.date_hour as date_time,
    -- A share of a mixed-decimals pool can top the range: keep its supply and balances.
    if(c.share_token_price_usd <= 1000000, c.share_token_price_usd, null) as price,
    [c.token0_representation_address, c.token1_representation_address] as tokens,
    cast([toDecimal256(c.balance0, 20), toDecimal256(c.balance1, 20)] as Array(Decimal(76, 20))) as balances,
    cast(toDecimal256(c.manager_total_supply, 20) as Nullable(Decimal(76, 20))) as total_supply,
    cast(null as Nullable(Decimal(76, 20))) as underlying_liquidity,
    cast([] as Array(Decimal(76, 20))) as underlying_balances,
    cast(null as Nullable(Decimal(76, 20))) as underlying_price,
    0 as priority
  FROM {{ ref('int_beefy_clm_subgraph__clm_stats_hourly') }} c
  WHERE c.manager_total_supply between 0 and 1000000000000
    AND {{ window['clm'] }}

  UNION ALL

  SELECT
    d.chain_id as chain_id,
    d.product_address as product_address,
    d.date_hour as date_time,
    cast(d.want_price_usd as Nullable(Float64)) as price,
    cast([] as Array(String)) as tokens,
    cast([] as Array(Decimal(76, 20))) as balances,
    cast(toDecimal256(d.underlying_total_supply, 20) as Nullable(Decimal(76, 20))) as total_supply,
    cast(null as Nullable(Decimal(76, 20))) as underlying_liquidity,
    cast([] as Array(Decimal(76, 20))) as underlying_balances,
    cast(null as Nullable(Decimal(76, 20))) as underlying_price,
    0 as priority
  FROM {{ ref('int_databarn_v1__vault_stats_hourly') }} d
  WHERE d.want_price_usd between 0 and 1000000
    AND d.underlying_total_supply between 0 and 1000000000000
    AND {{ window['dbv1'] }}
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(price, (priority, date_time)) as lp_price,
  argMax(tokens, (priority, date_time)) as breakdown_tokens,
  argMax(balances, (priority, date_time)) as breakdown_balances,
  argMax(total_supply, (priority, date_time)) as total_supply,
  argMax(underlying_liquidity, (priority, date_time)) as underlying_liquidity,
  argMax(underlying_balances, (priority, date_time)) as underlying_balances,
  argMax(underlying_price, (priority, date_time)) as underlying_price
FROM breakdown
GROUP BY chain_id, product_address, toStartOfHour(date_time)
