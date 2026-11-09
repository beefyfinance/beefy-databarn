{{

    config(
        materialized='view',
        tags=['api', 'revenue', 'summary']
    )
}}


SELECT 
    toFloat64(sum(revenue_usd) filter (where date_day >= today() - 30)) as revenue_usd_30d,
    toFloat64(sum(yield_usd) filter (where date_day >= today() - 7)) as yield_usd_7d,
    toFloat64(sum(bifi_buyback_usd) filter (where date_day >= today() - 7)) as bifi_buyback_usd_7d
FROM {{ ref('mart_revenue_summary_1d') }}
WHERE date_day >= today() - 30