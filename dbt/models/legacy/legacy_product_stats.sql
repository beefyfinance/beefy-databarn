{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('fact_product_stats_1h') }}
