{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('fact_chain_stats_1h') }}
