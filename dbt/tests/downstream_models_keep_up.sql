-- Fails when a model lags the model it reads from: that step stopped while its input kept moving.
-- Unlike models_are_fresh this holds when the sources themselves are stale.
-- Each lag allows for the hourly truncation of the downstream model. Sparse events get a day.

{% set edges = [
  ('fact_asset_price_5m', 'date_time', 'int_product_stats__share_price_1h', 'date_hour', 2),
  ('int_product_stats__share_price_1h', 'date_hour', 'int_product_stats__unified_1h', 'date_hour', 2),
  ('int_product_stats__tvl_1h', 'date_hour', 'int_product_stats__unified_1h', 'date_hour', 2),
  ('int_product_stats__apy_1h', 'date_hour', 'int_product_stats__unified_1h', 'date_hour', 2),
  ('stg_beefy_api__apy_breakdown', 'date_time', 'int_product_stats__apy_breakdown_1h', 'date_hour', 2),
  ('fact_asset_price_5m', 'date_time', 'int_product_stats__want_price_1h', 'date_hour', 2),
  ('int_product_stats__unified_1h', 'date_hour', 'fact_product_stats_1h', 'date_hour', 2),
  ('fact_product_stats_1h', 'date_hour', 'fact_chain_stats_1h', 'date_hour', 2),
  ('int_price_obs', 'ts', 'fact_asset_price_5m', 'date_time', 2),
  ('fact_asset_price_5m', 'date_time', 'fact_asset_price_1d', 'date_day', 48),
  ('int_harvests', 'txn_timestamp', 'int_product_stats__harvest_1h', 'date_hour', 24),
  ('int_harvests', 'txn_timestamp', 'fact_harvest_obs', 'txn_timestamp', 24),
  ('fact_account_balance_change_obs', 'block_timestamp', 'fact_account_balance_1d', 'date_day', 48),
] %}

select upstream, downstream, upstream_latest, downstream_latest, max_lag_hours
from (
  {% for upstream, upstream_column, downstream, downstream_column, max_lag_hours in edges %}
  select
    '{{ upstream }}' as upstream,
    '{{ downstream }}' as downstream,
    (select toDateTime(max({{ upstream_column }})) from {{ ref(upstream) }} where {{ upstream_column }} <= now()) as upstream_latest,
    (select toDateTime(max({{ downstream_column }})) from {{ ref(downstream) }} where {{ downstream_column }} <= now()) as downstream_latest,
    toInt64({{ max_lag_hours }}) as max_lag_hours
  {% if not loop.last %}union all{% endif %}
  {% endfor %}
)
where downstream_latest < upstream_latest - toIntervalHour(max_lag_hours)
