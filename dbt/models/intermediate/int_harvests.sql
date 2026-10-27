{{
  config(
    materialized='incremental',
    engine='MergeTree',
    tags=['intermediate', 'harvest'],
    unique_key=['network_id', 'vault_beefy_key', 'txn_timestamp', 'txn_hash', 'event_idx'],
    order_by=['txn_timestamp', 'network_id', 'block_number', 'txn_idx', 'event_idx'],
    on_schema_change='append_new_columns',
    incremental_strategy='delete+insert',
  )
}}

{% if is_incremental() %}
  {% set threshold_sql %}
    SELECT max(txn_timestamp) - INTERVAL 1 DAY FROM {{ this }}
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

-- Harvests with corrected prices. Same columns as stg_beefy_db__harvests.
-- want_price and native_price are the prices beefy-db stored at harvest time. Inside a known-bad
-- window they are replaced by int_price_oracle_corrections, on the harvest's 5-minute slot:
--   want_price   via the product's beefy_wants_price_oracle_key
--   native_price via the chain's beefy_native_price_oracle_key
-- Corrections are a tiny table: exact-key left joins. join_use_nulls=0, so a miss has price_source = ''.
-- Incremental: new harvests (1-day lookback) plus every harvest on a corrected slot, so adding a window
-- fixes old harvests on the next run. Removing one needs a reimport (make dbt reimport int_harvests+ <since>).

select
  h.network_id as network_id,
  h.block_number as block_number,
  h.txn_idx as txn_idx,
  h.event_idx as event_idx,
  h.txn_timestamp as txn_timestamp,
  h.txn_hash as txn_hash,
  h.vault_beefy_key as vault_beefy_key,
  h.call_fee as call_fee,
  h.gas_fee as gas_fee,
  h.raw_gas_fee as raw_gas_fee,
  h.platform_fee as platform_fee,
  h.strategist_fee as strategist_fee,
  h.harvest_amount as harvest_amount,
  h.is_cowllector as is_cowllector,
  h.strategist_address as strategist_address,
  if(want_corr.price_source != '', want_corr.price, h.want_price) as want_price,
  if(native_corr.price_source != '', native_corr.price, h.native_price) as native_price,
  want_corr.price_source != '' as is_want_price_corrected,
  native_corr.price_source != '' as is_native_price_corrected
from {{ ref('stg_beefy_db__harvests') }} h
left join {{ ref('int_product_keys') }} pk
  on h.network_id = pk.chain_id
  and h.vault_beefy_key = pk.beefy_key
left join {{ ref('int_chain_keys') }} c
  on h.network_id = c.chain_id
left join {{ ref('int_price_oracle_corrections') }} want_corr
  on pk.beefy_wants_price_oracle_key = want_corr.beefy_price_oracle_key
  and toStartOfFiveMinutes(h.txn_timestamp) = want_corr.date_time
left join {{ ref('int_price_oracle_corrections') }} native_corr
  on c.beefy_native_price_oracle_key = native_corr.beefy_price_oracle_key
  and toStartOfFiveMinutes(h.txn_timestamp) = native_corr.date_time
where
  {% if is_incremental() %}
  (
    (h.txn_timestamp >= toDateTime('{{ threshold }}') and h.txn_timestamp < now() + interval 1 day)
    or toStartOfFiveMinutes(h.txn_timestamp) in (select date_time from {{ ref('int_price_oracle_corrections') }})
  )
  and
  {% endif %}
  -- price is off for some harvests in the feed
  -- biggest real harvest we saw was this ($9M)
  -- https://etherscan.io/tx/0x31b8083e467ed217523655f9b26b71f154fd1358e633b275011123c268a88901
  -- next biggest harvest is bugged and shows as $32M
  toDecimal256(h.harvest_amount * if(want_corr.price_source != '', want_corr.price, h.want_price), 20) < 30_000_000.00
