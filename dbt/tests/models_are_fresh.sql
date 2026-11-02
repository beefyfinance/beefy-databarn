-- Fails when a model has no row newer than its allowed age: its run or the source behind it stopped.
-- Ages leave room for the 30-minute dbt schedule and for each source's cadence. Sparse or backfilled sources
-- (harvests, databarn v1, CLM subgraph) get a wider margin. See downstream_models_keep_up for a stuck model.

{% set models = [
  ('int_beefy_oracle_price', 'date_time', 6),
  ('int_product_share_price', 'date_time', 6),
  ('int_product_stats__share_price_hourly', 'date_hour', 6),
  ('int_product_stats__tvl_hourly', 'date_hour', 6),
  ('int_product_stats__apy_hourly', 'date_hour', 6),
  ('int_product_stats__lps_breakdown_hourly', 'date_hour', 6),
  ('int_product_stats__unified_hourly', 'date_hour', 6),
  ('product_stats', 'date_hour', 6),
  ('chain_stats', 'date_hour', 6),
  ('asset_price_5m', 'date_time', 6),
  ('asset_price_1h', 'date_hour', 6),
  ('asset_price_1d', 'date_day', 48),
  ('int_harvests', 'txn_timestamp', 24),
  ('int_yield', 'txn_timestamp', 24),
  ('harvests', 'txn_timestamp', 24),
  ('int_databarn_v1__vault_stats_hourly', 'date_hour', 72),
  ('int_beefy_clm_subgraph__clm_stats_hourly', 'date_hour', 72),
] %}

select model, latest, max_age_hours
from (
  {% for name, column, max_age_hours in models %}
  select
    '{{ name }}' as model,
    toDateTime(max({{ column }})) as latest,
    toInt64({{ max_age_hours }}) as max_age_hours
  from {{ ref(name) }}
  where {{ column }} <= now()
  {% if not loop.last %}union all{% endif %}
  {% endfor %}
)
where latest < now() - toIntervalHour(max_age_hours)
