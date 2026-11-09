-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['accounts', 'prices'],
    unique_key=['id'],
    order_by=['account_address', 'block_timestamp', 'id'],
    partition_by='toYear(block_timestamp)',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
    post_hook="{{ price_consumer_done('fact_account_balance_change_obs') }}",
  )
}}

{% if is_incremental() %}
  {% set threshold = run_query("SELECT toString(max(block_timestamp) - INTERVAL 1 DAY) FROM " ~ this).rows[0][0] %}
  {% set since = incremental_since_var() %}
  {% if since is not none %}
    {% set threshold = since %}
  {% endif %}
  {% set rp = price_reprice('fact_account_balance_change_obs', threshold, 'asset') %}
{% endif %}

-- One row per token balance change of an account (envio TokenBalanceChange), valued at the change's 5-minute slot
-- with fact_asset_price_5m: the token's price series is token.asset_id. Balances are in whole tokens (decimal-scaled)
-- and prices per whole token or share, so value = balance * price. USD columns are NULL when the token has no price
-- at that slot. For a user timeline: WHERE account_address = ... ORDER BY block_timestamp.
-- Incremental on block_timestamp (1-day lookback), plus the changes a resolver run re-priced (price_reprice: each
-- dirty asset from its first changed slot).

WITH changes AS (
  SELECT
    c.id AS id,
    c.chain_id AS chain_id,
    {{ evm_address('c.account_id') }} AS account_address,
    c.token_contract_address AS token_address,
    t.asset_id AS asset_id,
    c.block_number AS block_number,
    toDateTime(c.block_timestamp, 'UTC') AS block_timestamp,
    c.trx_hash AS trx_hash,
    c.log_index AS log_index,
    c.balance_before AS balance_before,
    c.balance_after AS balance_after,
    c.share_diff AS amount
  FROM {{ ref('stg_envio__token_balance_change') }} c
  LEFT JOIN {{ ref('dim_token') }} t
    ON t.chain_id = c.chain_id
    AND t.representation_address = c.token_contract_address
  {% if is_incremental() %}
  WHERE c.block_timestamp >= toDateTime64('{{ threshold }}', 3, 'UTC')
    OR {{ price_repriced(rp, 'ifNull(t.asset_id, 0)', "toDateTime(c.block_timestamp, 'UTC')") }}
  {% endif %}
),
-- Key lookup: the (asset, slot) pairs of the changes prune fact_asset_price_5m on its primary key.
prices AS (
  SELECT asset_id, date_time, price, price_source FROM {{ ref('fact_asset_price_5m') }}
  WHERE (asset_id, date_time) IN (SELECT asset_id, toStartOfFiveMinutes(block_timestamp) FROM changes WHERE asset_id IS NOT NULL)
)

SELECT
  c.id AS id,
  c.chain_id AS chain_id,
  c.account_address AS account_address,
  toLowCardinality(c.token_address) AS token_address,
  c.asset_id AS asset_id,
  c.block_number AS block_number,
  c.block_timestamp AS block_timestamp,
  c.trx_hash AS trx_hash,
  c.log_index AS log_index,
  c.balance_before AS balance_before,
  c.balance_after AS balance_after,
  c.amount AS amount,
  -- join_use_nulls = 0: a miss has price 0.
  if(p.price > 0, p.price, NULL) AS price_usd,
  if(p.price > 0, cast(p.price_source AS Nullable(String)), NULL) AS price_source,
  if(p.price > 0, toFloat64(c.amount) * p.price, NULL) AS amount_usd,
  if(p.price > 0, toFloat64(c.balance_after) * p.price, NULL) AS balance_after_usd
FROM changes c
LEFT JOIN prices p
  ON p.asset_id = c.asset_id
  AND p.date_time = toStartOfFiveMinutes(c.block_timestamp)
