-- 2026-11-09: dlt.beefy_api___lps_breakdown columns hinted as a bare "decimal" were created as Decimal(38, 9), which cut
-- totalSupply / underlyingLiquidity (exact 18-decimal strings from the API, down to 1e-18) to 9 decimals. The resource now
-- hints Decimal(76, 20) (sources/resources/beefy_api/snapshots.py); dlt does not alter existing columns, so widen them
-- here. Lossless. Run as a ClickHouse admin, with no dlt load running. Rows already loaded stay truncated (the API has no
-- history). (beefy_db tables are reflected from Postgres with their exact precision: not affected.)
ALTER TABLE dlt.beefy_api___lps_breakdown
    MODIFY COLUMN total_supply Nullable(Decimal(76, 20)),
    MODIFY COLUMN underlying_liquidity Nullable(Decimal(76, 20)),
    MODIFY COLUMN underlying_price Nullable(Decimal(76, 20))
SETTINGS mutations_sync = 2;
