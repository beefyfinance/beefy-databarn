-- One-off (2026-11-10): remove what product stats built from databarn v1 data past
-- int_product_stats__retirement.databarn_v1_until (retirement or chain eol + 1 month). delete+insert only replaces
-- the keys it re-inserts, so hours whose only source was v1 would stay. Unified (CoalescingMergeTree) keeps the last
-- non-NULL value and a merged row holds every source of its hour, so those hours are deleted from it whole. Run after
-- `dbt run -s int_product_stats__retirement` and right before the reimport, which re-inserts these hours from all
-- their other sources (unified reloads every source from incremental_since). No dbt job running. Idempotent.
DROP TABLE IF EXISTS dbt.tmp_dbv1_past_bound;
CREATE TABLE dbt.tmp_dbv1_past_bound ENGINE = MergeTree ORDER BY (date_hour, chain_id, product_address) AS
SELECT d.chain_id AS chain_id, d.product_address AS product_address, d.date_hour AS date_hour
FROM dbt.int_databarn_v1__vault_stats_1h d
INNER JOIN dbt.int_product_stats__retirement r ON r.chain_id = d.chain_id AND r.product_address = d.product_address
WHERE r.databarn_v1_until IS NOT NULL AND d.date_hour >= r.databarn_v1_until;
SELECT count() AS keys, uniqExact(chain_id, product_address) AS products, min(date_hour), max(date_hour) FROM dbt.tmp_dbv1_past_bound;
DELETE FROM dbt.int_databarn_v1__vault_stats_1h
WHERE (date_hour, chain_id, product_address) IN (SELECT date_hour, chain_id, product_address FROM dbt.tmp_dbv1_past_bound)
SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;
DELETE FROM dbt.int_product_stats__tvl_1h
WHERE (date_hour, chain_id, product_address) IN (SELECT date_hour, chain_id, product_address FROM dbt.tmp_dbv1_past_bound)
SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;
DELETE FROM dbt.int_product_stats__lps_breakdown_1h
WHERE (date_hour, chain_id, product_address) IN (SELECT date_hour, chain_id, product_address FROM dbt.tmp_dbv1_past_bound)
SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;
DELETE FROM dbt.int_product_stats__unified_1h
WHERE (date_hour, chain_id, product_address) IN (SELECT date_hour, chain_id, product_address FROM dbt.tmp_dbv1_past_bound)
SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;
DROP TABLE dbt.tmp_dbv1_past_bound;
