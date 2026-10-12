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


{% test latest_dlt_load_id_newest_completed(model, dlt_source, resources) %}

{#
  Independently recompute the newest completed load that wrote each listed
  resource (argMax by inserted_at) and require an exact match.

  Only attach this to append-style sources that keep historical _dlt_load_id
  (github_files, cctp). Do not attach to ReplacingMergeTree entity tables
  (beefy_api vaults/tokens/clm/cow/gov/boosts, beefy_db lookups): a later
  dlt load + merge drops the frozen load_id, and tests run after the model
  is intentionally frozen so concurrent pipelines can land a newer load.
  Snapshot tables (apy, tvl, ...) are also too large for this scan.

  ClickHouse-safe: EXCEPT DISTINCT, not FULL OUTER JOIN (join_use_nulls=0).
  Wrap each EXCEPT: UNION ALL and EXCEPT bind left-to-right at the same
  precedence, so an unparenthesized mix would be wrong.
#}

with expected as (
  {% for resource_name in resources %}
  {% if not loop.first %}union all{% endif %}
  select
    '{{ resource_name }}' as resource_name,
    argMax(l.load_id, l.inserted_at) as load_id
  from {{ source('dlt', dlt_source ~ '____dlt_loads') }} as l
  where l.status = 0
    and l.load_id in (
      select r._dlt_load_id
      from {{ source('dlt', dlt_source ~ '___' ~ resource_name) }} as r
      where r._dlt_load_id in (
        select load_id
        from {{ source('dlt', dlt_source ~ '____dlt_loads') }}
        where status = 0
      )
    )
  having count() > 0
  {% endfor %}
),
actual as (
  select resource_name, load_id
  from {{ model }}
  where resource_name in (
    {% for resource_name in resources %}
    '{{ resource_name }}'{% if not loop.last %},{% endif %}
    {% endfor %}
  )
)

select resource_name, load_id, 'model not expected' as reason
from (
  select resource_name, load_id from actual
  except distinct
  select resource_name, load_id from expected
)

union all

select resource_name, load_id, 'expected not in model' as reason
from (
  select resource_name, load_id from expected
  except distinct
  select resource_name, load_id from actual
)

{% endtest %}
