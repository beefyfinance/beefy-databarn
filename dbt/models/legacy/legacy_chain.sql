{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('dim_chain') }}
