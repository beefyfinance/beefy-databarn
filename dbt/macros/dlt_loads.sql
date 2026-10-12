{% macro dlt_recent_small_part_names(relation, max_rows=2000000, max_parts=128) %}

{#
  Newest MergeTree parts that are still small (not the daily OPTIMIZE blob).
  A skipped snapshot writes no new parts, so these names are the last
  successful loads. Empty list → caller must scan the whole table.
#}

  {% if not execute %}
    {{ return([]) }}
  {% endif %}
  {% set q %}
    SELECT name
    FROM system.parts
    WHERE database = '{{ relation.schema }}'
      AND table = '{{ relation.identifier }}'
      AND active
      AND rows < {{ max_rows }}
    ORDER BY max_block_number DESC
    LIMIT {{ max_parts }}
  {% endset %}
  {% set tbl = run_query(q) %}
  {% if tbl and tbl.columns | length > 0 %}
    {{ return(tbl.columns[0].values()) }}
  {% else %}
    {{ return([]) }}
  {% endif %}

{% endmacro %}


{% macro select_latest_dlt_loads(source_name, resources, exists_only_resources=None) %}

{#
  Newest completed load per resource in one dlt source.
  status = 0 is written only after the package succeeds.
  Load id is the newest completed load that actually wrote that resource
  (argMax by inserted_at). Optional API fetches skip a snapshot often;
  do not stamp the source's latest package in that case.

  `resources`: small entity tables — join `_dlt_load_id` to completed loads.

  `exists_only_resources`: snapshot tables that keep every etag. Read only
  the newest small parts (a skip writes no part). If OPTIMIZE has already
  merged those parts, fall back to a full-table join for that resource only.
#}

{%- set exists_only_resources = exists_only_resources or [] -%}
{%- set ns = namespace(emitted=false) -%}

WITH completed_loads AS (
  SELECT
    load_id,
    inserted_at
  FROM {{ source('dlt', source_name ~ '____dlt_loads') }}
  WHERE status = 0
)

{% for resource_name in resources %}
{% if ns.emitted %}
UNION ALL
{% endif %}
{% set ns.emitted = true %}
SELECT
  '{{ source_name }}' AS source_name,
  '{{ resource_name }}' AS resource_name,
  argMax(l.load_id, l.inserted_at) AS load_id,
  max(l.inserted_at) AS inserted_at
FROM {{ source('dlt', source_name ~ '___' ~ resource_name) }} AS r
INNER JOIN completed_loads AS l
  ON r._dlt_load_id = l.load_id
HAVING count() > 0
{% endfor %}

{% for resource_name in exists_only_resources %}
{% set rel = source('dlt', source_name ~ '___' ~ resource_name) %}
{% set parts = dlt_recent_small_part_names(rel) %}
{% if ns.emitted %}
UNION ALL
{% endif %}
{% set ns.emitted = true %}
SELECT
  '{{ source_name }}' AS source_name,
  '{{ resource_name }}' AS resource_name,
  argMax(l.load_id, l.inserted_at) AS load_id,
  max(l.inserted_at) AS inserted_at
FROM {{ rel }} AS r
INNER JOIN completed_loads AS l
  ON r._dlt_load_id = l.load_id
{% if parts %}
WHERE r._part IN (
  {% for part in parts %}
  '{{ part | replace("'", "\\'") }}'{% if not loop.last %},{% endif %}
  {% endfor %}
)
{% endif %}
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
  FINAL snapshot of a ReplacingMergeTree entity table.

  Do not filter to one completed `_dlt_load_id`: OPTIMIZE drops those parts
  and emptied gov/clm staging. Callers materialize as a table and LIMIT 1 BY
  their business key (API ids get reused/corrected across loads).
#}

SELECT *
FROM {{ source('dlt', source_name ~ '___' ~ resource_name) }}
FINAL

{% endmacro %}
