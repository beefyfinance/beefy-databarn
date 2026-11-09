{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('api_revenue_summary') }}
