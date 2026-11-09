{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('fact_buyback_obs') }}
