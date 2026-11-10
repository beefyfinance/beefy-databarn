-- Fails when a source of int_product_stats__unified_1h, or a column of fact_product_stats_1h, has no value newer
-- than its allowed age while the tables themselves keep moving. models_are_fresh only sees a table's newest row,
-- which any one source keeps fresh: the apy_breakdown branch of unified never ran for months, and the fact had no
-- apy for years, without that test failing.
-- Ages leave room for the 30-minute dbt schedule; harvests are sparse events and get a day.

{% set unified_sources = [
  ('tvl', 6), ('apy', 6), ('apy_breakdown', 6), ('lps_breakdown', 6), ('share_price', 6), ('want_price', 6), ('harvest', 24),
] %}

{#- One column per source family of the fact. want_tokens: the arrays come from lps_breakdown_1h, not unified. -#}
{% set fact_columns = [
  ('tvl_usd', 6), ('apy', 6), ('apr_vault', 6), ('fee_rate_performance', 6), ('share_price_usd', 6), ('want_price_usd', 6),
  ('want_total_supply', 6), ('notEmpty(want_tokens)', 6), ('harvest_count', 24),
] %}

select check_name, latest, max_age_hours
from (
  {% for source, max_age_hours in unified_sources %}
  select
    'int_product_stats__unified_1h source {{ source }}' as check_name,
    (select toDateTime(max(date_hour)) from {{ ref('int_product_stats__unified_1h') }}
     where source = '{{ source }}' and date_hour <= now() and date_hour >= now() - interval 7 day) as latest,
    toInt64({{ max_age_hours }}) as max_age_hours
  union all
  {% endfor %}
  {% for column, max_age_hours in fact_columns %}
  select
    'fact_product_stats_1h.{{ column }}' as check_name,
    (select toDateTime(max(date_hour)) from {{ ref('fact_product_stats_1h') }}
     where date_hour <= now() and date_hour >= now() - interval 7 day
       and {% if '(' in column %}{{ column }}{% else %}{{ column }} is not null{% endif %}) as latest,
    toInt64({{ max_age_hours }}) as max_age_hours
  {% if not loop.last %}union all{% endif %}
  {% endfor %}
)
where latest < now() - toIntervalHour(max_age_hours)
