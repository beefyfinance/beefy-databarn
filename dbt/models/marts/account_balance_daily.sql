-- depends_on: {{ ref('int_price_obs') }}
{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['marts', 'accounts', 'prices'],
    unique_key=['account_address', 'chain_id', 'token_address', 'date_day'],
    order_by=['account_address', 'chain_id', 'token_address', 'date_day'],
    partition_by='toYear(date_day)',
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
    query_settings={'max_bytes_before_external_group_by': 1000000000, 'max_bytes_before_external_sort': 1000000000},
    pre_hook="{{ account_balance_daily_clear_window() }}",
    post_hook="{{ price_consumer_done('account_balance_daily') }}",
  )
}}

{#- Window [d0, d1) of days built by this run:
    - incremental: from the day before the last day here, to today;
    - incremental_since / incremental_until (batched reimport or refresh, meta.batch): that range;
    - full build without vars: everything. -#}
{% set since = incremental_since_var() %}
{% set until = incremental_until_var() %}
{% set d0 = none %}
{% set d1 = none %}
{% if since is not none %}
  {% set d0 = run_query("SELECT toString(toDate('" ~ since ~ "'))").rows[0][0] if execute else '2000-01-01' %}
{% elif is_incremental() %}
  {% set d0 = run_query("SELECT toString(max(date_day) - 1) FROM " ~ this).rows[0][0] %}
{% endif %}
{#- Never past today: the last batch of a refresh ends at the end of its month. -#}
{% if until is not none %}
  {% set d1 = run_query("SELECT toString(least(toDate('" ~ until ~ "'), today() + 1))").rows[0][0] if execute else '2100-01-01' %}
{% endif %}
{% set carry = is_incremental() and d0 is not none %}
{% if is_incremental() and since is none %}
  {% set rp = price_reprice('account_balance_daily', d0 ~ ' 00:00:00', 'asset', 'day') %}
{% endif %}

-- One row per account, token and UTC day with a nonzero balance at the end of the day, valued at the day's close
-- (asset_price_1d) through token.asset_id. balance_usd is NULL when the token has no price that day.
-- Built from account_balance_changes: the balance at the end of each day with a change, carried to the next change.
-- Each run builds a window of days [d0, d1): positions open the day before d0 come from this table's own rows, so a
-- run reads only the window's changes. Incremental runs: from the day before the last day here, plus the rows of the
-- assets a resolver run re-priced, from the day of their first changed price (price_reprice). Full refresh and
-- reimport run by month (meta.batch). The window's days are deleted first (pre-hook), so a position closed by a late
-- change loses its carried rows; changes older than the window need a reimport (make dbt reimport
-- account_balance_daily <since>).

WITH
  day_ends AS (
    -- Balance at the end of each day with a change, in the window.
    SELECT
      account_address, chain_id, token_address,
      any(asset_id) AS asset_id,
      toDate(block_timestamp) AS d,
      argMax(balance_after, (block_timestamp, log_index, id)) AS balance
    FROM {{ ref('account_balance_changes') }}
    WHERE 1
      {% if d0 is not none and carry %}AND block_timestamp >= toDateTime('{{ d0 }} 00:00:00', 'UTC'){% endif %}
      {% if d1 is not none %}AND block_timestamp < toDateTime('{{ d1 }} 00:00:00', 'UTC'){% endif %}
    GROUP BY account_address, chain_id, token_address, d
    {% if carry %}
    UNION ALL
    -- Positions open at the end of the day before the window.
    SELECT account_address, chain_id, token_address, asset_id, date_day AS d, balance
    FROM {{ this }}
    WHERE date_day = toDate('{{ d0 }}') - 1
    {% endif %}
  ),
  segments AS (
    -- A balance holds from its day to the day before the next change day, or to the window end.
    SELECT
      *,
      leadInFrame(d, 1, {% if d1 is not none %}toDate('{{ d1 }}'){% else %}today() + 1{% endif %}) OVER (
        PARTITION BY account_address, chain_id, token_address ORDER BY d
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
      ) AS next_d
    FROM day_ends
  ),
  days AS (
    SELECT
      account_address, chain_id, token_address, asset_id, balance,
      arrayJoin(arrayMap(i -> d + i, range(toUInt32(greatest(dateDiff('day', d, next_d), 0))))) AS date_day
    FROM segments
    WHERE balance != 0
  ),
  rows AS (
    SELECT * FROM days
    {% if d0 is not none %}WHERE date_day >= toDate('{{ d0 }}'){% endif %}
    {% if is_incremental() and since is none %}
    UNION ALL
    -- Re-priced days before the window: same balances, new close.
    SELECT account_address, chain_id, token_address, asset_id, balance, date_day
    FROM {{ this }}
    WHERE date_day < toDate('{{ d0 }}')
      AND {{ price_repriced(rp, 'ifNull(asset_id, 0)', "toDateTime(date_day, 'UTC')") }}
    {% endif %}
  )

SELECT
  r.account_address AS account_address,
  r.chain_id AS chain_id,
  r.token_address AS token_address,
  r.date_day AS date_day,
  r.asset_id AS asset_id,
  r.balance AS balance,
  -- join_use_nulls = 0: a miss has close 0.
  if(p.close > 0, p.close, NULL) AS price_usd,
  if(p.close > 0, toFloat64(r.balance) * p.close, NULL) AS balance_usd
FROM rows r
LEFT JOIN (
  SELECT asset_id, date_day, close FROM {{ ref('asset_price_1d') }}
  WHERE 1
    {% if d0 is not none %}
    AND (date_day >= toDate('{{ d0 }}'){% if is_incremental() and since is none %} OR {{ price_repriced(rp, 'asset_id', "toDateTime(date_day, 'UTC')") }}{% endif %})
    {% endif %}
    {% if d1 is not none %}AND date_day < toDate('{{ d1 }}'){% endif %}
) p
  ON p.asset_id = r.asset_id
  AND p.date_day = r.date_day
