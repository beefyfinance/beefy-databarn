{{
  config(
    materialized='incremental',
    tags=['intermediate', 'investor', 'timeline'],
    unique_key=['account_id', 'datetime', 'product_address', 'transaction_hash', 'log_index'],
    engine='ReplacingMergeTree(__rmt_version)',
    order_by=['account_id', 'datetime', 'product_address', 'transaction_hash', 'log_index'],
    on_schema_change='fail',
  )
}}

{% if this %}
  {% set relation_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
  {% if relation_exists %}
    {% set max_date_sql %}
      select max(cast(datetime as Nullable(DateTime64(3, 'UTC')))) as max_datetime
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
{% else %}
  {% set max_date = '2021-07-31 19:30:00' %}
{% endif %}
{% set start_date = max_date %}

-- Intermediate model: Historical investor timeline data
-- Incremental table for processing historical data in batches to avoid memory issues
-- Processes maximum 1 month per run to control memory usage
-- Excludes last 15 days to avoid overlap with recent view
-- Filters all tables (balance changes and prices) by datetime for efficiency
-- Allows overlap with recent view - deduplication handled in unified mart
-- Optimized for querying by user address (account_id) and date

WITH filtered_balance_changes AS (
  SELECT
    *
  FROM {{ ref('stg_envio__token_balance_change') }}
  WHERE
    -- Process maximum 1 month per run, but exclude last 15 days
    block_timestamp >= toDateTime('{{ start_date }}')
    AND block_timestamp < LEAST(
      toDateTime('{{ start_date }}') + INTERVAL 1 MONTH,
      now() - INTERVAL 7 DAY
    )
),

filtered_prices AS (
  SELECT
    chain_id,
    token_address,
    date_time,
    price
  FROM {{ ref('int_product_price') }}
  WHERE
    -- Filter price table by datetime range for efficiency
    -- Match the date range of balance changes: start_date to start_date + 1 month, but not within last 15 days
    date_time >= toDateTime('{{ start_date }}')
    AND date_time < LEAST(
      toDateTime('{{ start_date }}') + INTERVAL 1 MONTH,
      now() - INTERVAL 7 DAY
    )
)

SELECT
  -- replacing merge tree version dedup field
  now() as __rmt_version,

  tbc.block_timestamp as datetime,
  tbc.account_id as account_id,
  p.beefy_key as product_key,
  p.display_name as product_display_name,
  tbc.chain_id as chain_id,
  c.chain_name as chain_name,
  p.product_type as product_type,
  tbc.token_contract_address as product_address,

  NOT p.is_active as is_eol,
  NOT p.is_active as is_dashboard_eol,
  
  tbc.block_number as block_number,
  tbc.trx_hash as transaction_hash,
  tbc.log_index as log_index,

  cast(NULL as Nullable(Decimal(76, 20))) as share_to_underlying_price,
  cast(NULL as Nullable(Decimal(76, 20))) as underlying_to_usd_price,
  share_price.price as share_to_usd_price,

  -- Balances
  tbc.balance_after as share_balance_after,
  tbc.balance_before as share_balance_before,
  tbc.share_diff as share_balance_diff,

  cast(NULL as Nullable(Decimal(76, 20))) as underlying_balance_after,
  cast(NULL as Nullable(Decimal(76, 20))) as underlying_balance_before,
  cast(NULL as Nullable(Decimal(76, 20))) as underlying_balance_diff,

  {{ to_decimal('tbc.balance_before * coalesce(share_price.price, 0)') }} as usd_balance_before,
  {{ to_decimal('tbc.balance_after * coalesce(share_price.price, 0)') }} as usd_balance_after,
  {{ to_decimal('(tbc.balance_after - tbc.balance_before) * coalesce(share_price.price, 0)') }} as usd_balance_diff
FROM filtered_balance_changes tbc
INNER JOIN {{ ref('product') }} p
  ON tbc.chain_id = p.chain_id
  AND tbc.token_contract_address = p.product_address
INNER JOIN {{ ref('chain') }} c
  ON tbc.chain_id = c.chain_id
ASOF LEFT JOIN filtered_prices share_price
  ON tbc.chain_id = share_price.chain_id
  AND tbc.token_contract_address = share_price.token_address
  AND tbc.block_timestamp >= share_price.date_time

