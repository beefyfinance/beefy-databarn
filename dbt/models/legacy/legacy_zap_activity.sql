{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('fact_zap_obs') }}
