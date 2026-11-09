-- One-off (2026-11-09): switch existing incremental tables to the types the models now produce:
-- LowCardinality for repeated strings, Float64 for prices and USD values (token amounts stay Decimal(76, 20)).
-- `table` models (int_product_stats__harvest_1h, __yield_1h, int_yield, fact_harvest_obs, stg_beefy_db__harvests,
-- fact_chain_stats_1h) pick the new types up on their next rebuild. Run with no dbt job running. Idempotent.
ALTER TABLE dbt.int_product_stats__apy_1h MODIFY COLUMN product_address LowCardinality(String) SETTINGS mutations_sync = 2;
ALTER TABLE dbt.int_product_stats__apy_breakdown_1h MODIFY COLUMN product_address LowCardinality(String) SETTINGS mutations_sync = 2;
ALTER TABLE dbt.int_databarn_v1__vault_stats_1h MODIFY COLUMN product_address LowCardinality(String) SETTINGS mutations_sync = 2;
ALTER TABLE dbt.int_harvests
    MODIFY COLUMN vault_beefy_key LowCardinality(String),
    MODIFY COLUMN strategist_address LowCardinality(Nullable(String)),
    MODIFY COLUMN want_price Float64,
    MODIFY COLUMN native_price Float64,
    MODIFY COLUMN platform_fee_price Float64
SETTINGS mutations_sync = 2;
ALTER TABLE dbt.int_product_stats__lps_breakdown_1h
    MODIFY COLUMN product_address LowCardinality(String),
    MODIFY COLUMN breakdown_tokens Array(LowCardinality(String)),
    MODIFY COLUMN underlying_price Nullable(Float64)
SETTINGS mutations_sync = 2;
ALTER TABLE dbt.int_product_stats__tvl_1h MODIFY COLUMN product_address LowCardinality(String) SETTINGS mutations_sync = 2;
ALTER TABLE dbt.int_product_stats__share_price_1h
    MODIFY COLUMN product_address LowCardinality(String),
    MODIFY COLUMN share_token_price_usd Float64
SETTINGS mutations_sync = 2;
ALTER TABLE dbt.int_product_stats__unified_1h
    MODIFY COLUMN product_address LowCardinality(String),
    MODIFY COLUMN source LowCardinality(String),
    MODIFY COLUMN breakdown_tokens Array(LowCardinality(String)),
    MODIFY COLUMN underlying_price Nullable(Float64),
    MODIFY COLUMN underlying_token_price_usd Nullable(Float64),
    MODIFY COLUMN underlying_amount_compounded_usd Nullable(Float64),
    MODIFY COLUMN harvest_call_fee_usd Nullable(Float64),
    MODIFY COLUMN gas_fee_usd Nullable(Float64),
    MODIFY COLUMN platform_fee_usd Nullable(Float64),
    MODIFY COLUMN strategist_fee_usd Nullable(Float64),
    MODIFY COLUMN harvest_amount_usd Nullable(Float64),
    MODIFY COLUMN share_token_price_usd Nullable(Float64)
SETTINGS mutations_sync = 2;
ALTER TABLE databarn.fact_product_stats_1h
    MODIFY COLUMN chain_name LowCardinality(String) CODEC(LZ4),
    MODIFY COLUMN product_type LowCardinality(String) CODEC(LZ4),
    MODIFY COLUMN beefy_key LowCardinality(String) CODEC(LZ4),
    MODIFY COLUMN product_address LowCardinality(String) CODEC(LZ4),
    MODIFY COLUMN display_name LowCardinality(String) CODEC(LZ4),
    MODIFY COLUMN platform_id LowCardinality(Nullable(String)) CODEC(LZ4),
    MODIFY COLUMN breakdown_tokens Array(LowCardinality(String)) CODEC(LZ4),
    MODIFY COLUMN underlying_price Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN underlying_token_price_usd Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN underlying_amount_compounded_usd Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN harvest_call_fee_usd Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN gas_fee_usd Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN platform_fee_usd Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN strategist_fee_usd Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN harvest_amount_usd Nullable(Float64) CODEC(ZSTD(3)),
    MODIFY COLUMN share_token_price_usd Nullable(Float64) CODEC(ZSTD(3))
SETTINGS mutations_sync = 2;
ALTER TABLE databarn.fact_account_balance_change_obs MODIFY COLUMN token_address LowCardinality(String) SETTINGS mutations_sync = 2;
ALTER TABLE databarn.fact_account_balance_1d MODIFY COLUMN token_address LowCardinality(String) SETTINGS mutations_sync = 2;
