{{ legacy_proxy_guard() }}
SELECT * EXCEPT (as_of), as_of AS date_hour
FROM {{ ref('fact_product_stats_latest') }}
