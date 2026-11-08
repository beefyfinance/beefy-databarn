-- Fails when a model lags the model it reads from: that step stopped while its input kept moving.
-- Unlike models_are_fresh this holds when the sources themselves are stale.
-- Each lag allows for the hourly truncation of the downstream model. Sparse events get a day.

{% set edges = [
  ('asset_price_5m', 'date_time', 'int_product_stats__share_price_hourly', 'date_hour', 2),
  ('int_product_stats__share_price_hourly', 'date_hour', 'int_product_stats__unified_hourly', 'date_hour', 2),
  ('int_product_stats__tvl_hourly', 'date_hour', 'int_product_stats__unified_hourly', 'date_hour', 2),
  ('int_product_stats__apy_hourly', 'date_hour', 'int_product_stats__unified_hourly', 'date_hour', 2),
  ('int_product_stats__unified_hourly', 'date_hour', 'product_stats', 'date_hour', 2),
  ('product_stats', 'date_hour', 'chain_stats', 'date_hour', 2),
  ('int_price_obs', 'ts', 'asset_price_5m', 'date_time', 2),
  ('asset_price_5m', 'date_time', 'asset_price_1d', 'date_day', 48),
  ('int_harvests', 'txn_timestamp', 'int_yield', 'txn_timestamp', 24),
  ('int_yield', 'txn_timestamp', 'harvests', 'txn_timestamp', 24),
  ('account_balance_changes', 'block_timestamp', 'account_balance_daily', 'date_day', 48),
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
