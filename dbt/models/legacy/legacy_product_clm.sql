{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('dim_product_clm') }}
