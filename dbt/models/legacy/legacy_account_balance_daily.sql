{{ legacy_proxy_guard() }}
SELECT * FROM {{ ref('fact_account_balance_1d') }}
