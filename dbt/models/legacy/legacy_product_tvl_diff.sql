{{ legacy_proxy_guard() }}
SELECT * EXCEPT (as_of), as_of AS ref_hour
FROM {{ ref('mart_product_tvl_change_latest') }}
