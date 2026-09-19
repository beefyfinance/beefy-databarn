{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.id as String) as id,
  ifNull(t.name, 'Unknown') as name,
  t.website,
  t.twitter,
  t.documentation,
  t.description,
  t.type,
  JSONExtract(coalesce(t.risks, '[]'), 'Array(String)') as risks
FROM {{ source('dlt', 'github_files___beefy_platforms') }} t FINAL

