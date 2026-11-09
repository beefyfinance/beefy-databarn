{{ legacy_proxy_guard() }}
SELECT * EXCEPT (want_price)
FROM {{ ref('fact_harvest_obs') }}
