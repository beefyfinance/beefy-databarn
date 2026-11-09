{{ legacy_proxy_guard() }}
SELECT asset_id, as_of AS date_time, price, price_source
FROM {{ ref('fact_asset_price_latest') }}
