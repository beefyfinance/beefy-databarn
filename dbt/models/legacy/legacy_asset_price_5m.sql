{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('fact_asset_price_5m') }}
