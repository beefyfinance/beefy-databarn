{{
  config(
    materialized='view',
  )
}}

-- Block ranges for product imports, date ranges for oracle:price. An import covers a range even
-- when it fails: what was really imported is covered_ranges minus to_retry_ranges.
SELECT
  cast(t.import_key as String) AS import_key,
  cast(JSONExtractString(t.import_data, 'type') as String) AS import_type,
  JSONExtract(t.import_data, 'productId', 'Nullable(Int64)') AS product_id,
  JSONExtract(t.import_data, 'priceFeedId', 'Nullable(Int64)') AS price_feed_id,
  nullIf(JSONExtractString(t.import_data, 'chain'), '') AS chain_key,
  JSONExtract(t.import_data, 'contractCreatedAtBlock', 'Nullable(Int64)') AS contract_created_at_block,
  parseDateTime64BestEffortOrNull(JSONExtractString(t.import_data, 'contractCreationDate'), 3, 'UTC') AS contract_creation_datetime,
  JSONExtract(t.import_data, 'chainLatestBlockNumber', 'Nullable(Int64)') AS chain_latest_block_number,
  parseDateTime64BestEffortOrNull(JSONExtractString(t.import_data, 'firstDate'), 3, 'UTC') AS first_datetime,
  parseDateTime64BestEffortOrNull(JSONExtractString(t.import_data, 'ranges', 'lastImportDate'), 3, 'UTC') AS last_import_datetime,
  cast(JSONExtractRaw(t.import_data, 'ranges', 'coveredRanges') as String) AS covered_ranges,
  cast(JSONExtractRaw(t.import_data, 'ranges', 'toRetry') as String) AS to_retry_ranges
FROM {{ source('dlt', 'databarn_v1___import_states') }} t FINAL
