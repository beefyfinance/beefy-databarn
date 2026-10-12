{% macro dlt_recent_small_parts_sql(relation, max_rows=2000000, block_window=2048) %}

{#
  Small parts near the table's newest block. Skip writes no part, so those
  names are the last successful loads. Exclude leftover low-block parts
  (e.g. all_2_2_0) after OPTIMIZE has folded recent rows into a giant part.
  Evaluated at query time so compiled SQL is not bound to one server's parts.
#}

  SELECT name
  FROM system.parts
  WHERE database = '{{ relation.schema }}'
    AND table = '{{ relation.identifier }}'
    AND active
    AND rows < {{ max_rows }}
    AND max_block_number >= (
      SELECT max(max_block_number) - {{ block_window }}
      FROM system.parts
      WHERE database = '{{ relation.schema }}'
        AND table = '{{ relation.identifier }}'
        AND active
    )

{% endmacro %}


{% macro select_latest_dlt_loads(source_name, resources, exists_only_resources=None) %}

{#
  Newest completed load per resource in one dlt source.
  status = 0 is written only after the package succeeds.
  Load id is the newest completed load that actually wrote that resource
  (argMax by inserted_at). Optional API fetches skip a snapshot often;
  do not stamp the source's latest package in that case.

  `resources`: small entity tables — join `_dlt_load_id` to completed loads.

  `exists_only_resources`: snapshot tables that keep every etag. Restrict to
  recent small parts. If none exist (everything already OPTIMIZEd), scan the
  whole table for that resource only.
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
WHERE r._part IN (
  {{ dlt_recent_small_parts_sql(rel) }}
)
  OR (
    SELECT count()
    FROM (
      {{ dlt_recent_small_parts_sql(rel) }}
    )
  ) = 0
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
