-- One-off (2026-11-10), before the first dbt run of the new code, with no dbt job running. Idempotent.
-- int_product_stats__unified_1h loses its array and yield columns and gains want_price_usd. Its `append` strategy
-- ignores on_schema_change (dbt-clickhouse inserts with the table's column list), so the table must match the model
-- before the next run.
-- fact_chain_stats_1h (a table) and the views pick up their renamed columns on their next run.
ALTER TABLE dbt.int_product_stats__unified_1h
    DROP COLUMN IF EXISTS breakdown_tokens,
    DROP COLUMN IF EXISTS breakdown_balances,
    DROP COLUMN IF EXISTS underlying_balances,
    DROP COLUMN IF EXISTS underlying_amount_compounded,
    DROP COLUMN IF EXISTS underlying_token_price_usd,
    DROP COLUMN IF EXISTS underlying_amount_compounded_usd,
    DROP COLUMN IF EXISTS harvest_vault_count,
    ADD COLUMN IF NOT EXISTS want_price_usd Nullable(Float64) AFTER share_token_price_usd
SETTINGS mutations_sync = 2;
-- The yield source duplicated int_product_stats__harvest_1h (same int_harvests rows, last harvest of the hour
-- instead of the sum). Its models are deleted.
DROP TABLE IF EXISTS dbt.int_product_stats__yield_1h;
DROP TABLE IF EXISTS dbt.int_yield;

-- fact_product_stats_1h: rename and reorder in place (metadata only: same types and codecs, the data stays), so the table keeps its history
-- without a full refresh. Values that change (apy merged with the API totalApy, want_price_usd with its fallback,
-- the arrays from lps_breakdown_1h) are rewritten by the reimport.
ALTER TABLE databarn.fact_product_stats_1h
    RENAME COLUMN IF EXISTS share_token_price_usd TO share_price_usd,
    RENAME COLUMN IF EXISTS lp_price TO want_price_usd,
    RENAME COLUMN IF EXISTS vault_apy TO apy_vault,
    RENAME COLUMN IF EXISTS vault_apr TO apr_vault,
    RENAME COLUMN IF EXISTS trading_apr TO apr_trading,
    RENAME COLUMN IF EXISTS clm_apr TO apr_clm,
    RENAME COLUMN IF EXISTS reward_pool_apr TO apr_reward_pool,
    RENAME COLUMN IF EXISTS reward_pool_trading_apr TO apr_reward_pool_trading,
    RENAME COLUMN IF EXISTS liquid_staking_apr TO apr_liquid_staking,
    RENAME COLUMN IF EXISTS composable_pool_apr TO apr_composable_pool,
    RENAME COLUMN IF EXISTS merkl_apr TO apr_merkl,
    RENAME COLUMN IF EXISTS linea_ignition_apr TO apr_linea_ignition,
    RENAME COLUMN IF EXISTS beefy_performance_fee TO fee_rate_performance,
    RENAME COLUMN IF EXISTS lp_fee TO fee_rate_lp,
    RENAME COLUMN IF EXISTS total_supply TO want_total_supply,
    RENAME COLUMN IF EXISTS breakdown_tokens TO want_tokens,
    RENAME COLUMN IF EXISTS breakdown_balances TO want_balances,
    RENAME COLUMN IF EXISTS underlying_liquidity TO clm_pool_liquidity,
    RENAME COLUMN IF EXISTS underlying_price TO clm_pool_liquidity_price_usd,
    RENAME COLUMN IF EXISTS underlying_balances TO clm_pool_balances,
    RENAME COLUMN IF EXISTS harvest_txn_count TO harvest_count,
    RENAME COLUMN IF EXISTS gas_fee TO harvest_gas_fee,
    RENAME COLUMN IF EXISTS gas_fee_usd TO harvest_gas_fee_usd,
    RENAME COLUMN IF EXISTS platform_fee TO harvest_platform_fee,
    RENAME COLUMN IF EXISTS platform_fee_usd TO harvest_platform_fee_usd,
    RENAME COLUMN IF EXISTS strategist_fee TO harvest_strategist_fee,
    RENAME COLUMN IF EXISTS strategist_fee_usd TO harvest_strategist_fee_usd,
    RENAME COLUMN IF EXISTS avg_native_price TO harvest_native_price_usd
SETTINGS mutations_sync = 2;
ALTER TABLE databarn.fact_product_stats_1h
    DROP COLUMN IF EXISTS total_apy,
    DROP COLUMN IF EXISTS underlying_amount_compounded,
    DROP COLUMN IF EXISTS underlying_token_price_usd,
    DROP COLUMN IF EXISTS underlying_amount_compounded_usd,
    DROP COLUMN IF EXISTS harvest_vault_count
SETTINGS mutations_sync = 2;
ALTER TABLE databarn.fact_product_stats_1h
    MODIFY COLUMN tvl_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER date_hour,
    MODIFY COLUMN apy Nullable(Float64) CODEC(ZSTD(3)) AFTER tvl_usd,
    MODIFY COLUMN share_price_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER apy,
    MODIFY COLUMN want_price_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER share_price_usd,
    MODIFY COLUMN apy_vault Nullable(Float64) CODEC(ZSTD(3)) AFTER want_price_usd,
    MODIFY COLUMN apr_vault Nullable(Float64) CODEC(ZSTD(3)) AFTER apy_vault,
    MODIFY COLUMN apr_trading Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_vault,
    MODIFY COLUMN apr_clm Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_trading,
    MODIFY COLUMN apr_reward_pool Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_clm,
    MODIFY COLUMN apr_reward_pool_trading Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_reward_pool,
    MODIFY COLUMN apr_liquid_staking Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_reward_pool_trading,
    MODIFY COLUMN apr_composable_pool Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_liquid_staking,
    MODIFY COLUMN apr_merkl Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_composable_pool,
    MODIFY COLUMN apr_linea_ignition Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_merkl,
    MODIFY COLUMN fee_rate_performance Nullable(Float64) CODEC(ZSTD(3)) AFTER apr_linea_ignition,
    MODIFY COLUMN fee_rate_lp Nullable(Float64) CODEC(ZSTD(3)) AFTER fee_rate_performance,
    MODIFY COLUMN compoundings_per_year Nullable(Int64) CODEC(ZSTD(3)) AFTER fee_rate_lp,
    MODIFY COLUMN want_total_supply Nullable(Decimal(76, 20)) CODEC(ZSTD(3)) AFTER compoundings_per_year,
    MODIFY COLUMN want_tokens Array(LowCardinality(String)) CODEC(LZ4) AFTER want_total_supply,
    MODIFY COLUMN want_balances Array(Decimal(76, 20)) CODEC(LZ4) AFTER want_tokens,
    MODIFY COLUMN clm_pool_liquidity Nullable(Decimal(76, 20)) CODEC(ZSTD(3)) AFTER want_balances,
    MODIFY COLUMN clm_pool_liquidity_price_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER clm_pool_liquidity,
    MODIFY COLUMN clm_pool_balances Array(Decimal(76, 20)) CODEC(LZ4) AFTER clm_pool_liquidity_price_usd,
    MODIFY COLUMN harvest_count Nullable(UInt64) CODEC(ZSTD(3)) AFTER clm_pool_balances,
    MODIFY COLUMN harvest_amount Nullable(Decimal(76, 20)) CODEC(ZSTD(3)) AFTER harvest_count,
    MODIFY COLUMN harvest_amount_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER harvest_amount,
    MODIFY COLUMN harvest_call_fee Nullable(Decimal(76, 20)) CODEC(ZSTD(3)) AFTER harvest_amount_usd,
    MODIFY COLUMN harvest_call_fee_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER harvest_call_fee,
    MODIFY COLUMN harvest_gas_fee Nullable(Decimal(76, 20)) CODEC(ZSTD(3)) AFTER harvest_call_fee_usd,
    MODIFY COLUMN harvest_gas_fee_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER harvest_gas_fee,
    MODIFY COLUMN harvest_platform_fee Nullable(Decimal(76, 20)) CODEC(ZSTD(3)) AFTER harvest_gas_fee_usd,
    MODIFY COLUMN harvest_platform_fee_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER harvest_platform_fee,
    MODIFY COLUMN harvest_strategist_fee Nullable(Decimal(76, 20)) CODEC(ZSTD(3)) AFTER harvest_platform_fee_usd,
    MODIFY COLUMN harvest_strategist_fee_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER harvest_strategist_fee,
    MODIFY COLUMN harvest_native_price_usd Nullable(Float64) CODEC(ZSTD(3)) AFTER harvest_strategist_fee_usd
SETTINGS mutations_sync = 2;

-- int_product_stats__tracking became int_product_stats__retirement (the product stats cutoff is reverted).
DROP TABLE IF EXISTS dbt.int_product_stats__tracking;
