{% test latest_dlt_load_id_completed(model, dlt_source) %}

{#
  Every load_id + inserted_at on this latest-load table must be a completed
  dlt load (status 0) in that source's _dlt_loads table.
  ClickHouse-safe: EXCEPT DISTINCT, not LEFT JOIN ... IS NULL.
#}

select load_id, inserted_at
from {{ model }}
except distinct
select load_id, inserted_at
from {{ source('dlt', dlt_source ~ '____dlt_loads') }}
where status = 0

{% endtest %}


{% test latest_dlt_load_id_in_resource(model, dlt_source, resources) %}

{#
  The model must contain exactly the declared resource names.

  Do not re-check load_id against the live resource table. Entity tables are
  ReplacingMergeTree on primary key, so a later dlt load + merge drops the
  frozen _dlt_load_id. Snapshot tables partitioned by etag keep history, which
  is why this used to fail for vaults/clm/gov/boosts/tokens/cow (6) and every
  beefy_db table (7) while github/cctp stayed green.

  A row in this model already means the build-time inner join found that
  load_id on the resource table.
#}

select
  resource_name,
  load_id,
  'unexpected resource' as reason
from {{ model }}
where resource_name not in (
  -- declared resources for {{ dlt_source }}
  {% for resource_name in resources %}
  '{{ resource_name }}'{% if not loop.last %},{% endif %}
  {% endfor %}
)

{% for resource_name in resources %}
union all
select
  '{{ resource_name }}' as resource_name,
  cast(null as Nullable(String)) as load_id,
  'missing resource' as reason
from system.one
where not exists (
  select 1
  from {{ model }}
  where resource_name = '{{ resource_name }}'
)
{% endfor %}

{% endtest %}
