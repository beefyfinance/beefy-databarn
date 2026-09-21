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
  Each resource's load_id must exist on that resource table, and the model
  must contain exactly the declared resource names.
  Each EXCEPT is wrapped so UNION ALL cannot associate with EXCEPT.
#}

{% for resource_name in resources %}
select * from (
  select
    resource_name,
    load_id,
    'load_id not in resource table' as reason
  from {{ model }}
  where resource_name = '{{ resource_name }}'
  except distinct
  select
    '{{ resource_name }}' as resource_name,
    _dlt_load_id as load_id,
    'load_id not in resource table' as reason
  from {{ source('dlt', dlt_source ~ '___' ~ resource_name) }}
)
{% if not loop.last %}
union all
{% endif %}
{% endfor %}

union all

select
  resource_name,
  load_id,
  'unexpected resource' as reason
from {{ model }}
where resource_name not in (
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
where not exists (
  select 1
  from {{ model }}
  where resource_name = '{{ resource_name }}'
)
{% endfor %}

{% endtest %}
