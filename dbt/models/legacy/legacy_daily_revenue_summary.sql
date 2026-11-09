{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('mart_revenue_summary_1d') }}
