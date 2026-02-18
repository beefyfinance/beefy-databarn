{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['dimension', 'platforms']
  )
}}

-- Dimension table: Platform reference data
-- This table provides platform metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  t.id as platform_id,
  t.name as platform_name,
  t.website,
  t.twitter,
  t.documentation,
  t.description,
  t.type as platform_type,
  t.risks
FROM {{ ref('stg_github_files___beefy_platforms') }} t

