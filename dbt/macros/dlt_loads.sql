{% macro select_latest_dlt_loads(source_name, resources) %}

{#
  Newest completed load per resource in one dlt source.
  status = 0 is written only after the package succeeds.
  Load id is the newest completed load that actually wrote that resource,
  so a later package that skipped it cannot empty the staging copy.
#}

WITH completed_loads AS (
  SELECT
    load_id,
    inserted_at
  FROM {{ source('dlt', source_name ~ '____dlt_loads') }}
  WHERE status = 0
)

{% for resource_name in resources %}
{% if not loop.first %}UNION ALL{% endif %}
SELECT
  '{{ source_name }}' AS source_name,
  '{{ resource_name }}' AS resource_name,
  argMax(l.load_id, l.inserted_at) AS load_id,
  max(l.inserted_at) AS inserted_at
FROM (
  SELECT DISTINCT _dlt_load_id AS load_id
  FROM {{ source('dlt', source_name ~ '___' ~ resource_name) }}
) AS t
INNER JOIN completed_loads AS l
  ON t.load_id = l.load_id
HAVING count() > 0
{% endfor %}

{% endmacro %}


{% macro latest_dlt_load_id(source_name, resource_name) -%}
(
  SELECT load_id
  FROM {{ ref('stg_' ~ source_name ~ '__latest_dlt_loads') }}
  WHERE resource_name = '{{ resource_name }}'
)
{%- endmacro %}


{% macro latest_completed_dlt_rows(source_name, resource_name) %}

{#
  Newest completed row per `id` for a ReplacingMergeTree entity table.

  Do not filter to a single frozen `_dlt_load_id`. OPTIMIZE FINAL replaces
  older parts and drops that load_id, which emptied gov/clm staging and
  dropped those products from int_product_keys. Ignoring in-flight loads
  (status != 0) still avoids a half-written package hiding completed rows.
#}

SELECT t.*
FROM {{ source('dlt', source_name ~ '___' ~ resource_name) }} AS t
INNER JOIN {{ ref('stg_' ~ source_name ~ '__dlt_loads') }} AS l
  ON t._dlt_load_id = l.load_id
WHERE l.status = 0
ORDER BY l.inserted_at DESC
LIMIT 1 BY t.id

{% endmacro %}
