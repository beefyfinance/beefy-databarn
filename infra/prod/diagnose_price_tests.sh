#!/bin/bash
# Read-only: inputs behind the two failing price tests, plus vaults sharing a moo token.
# Run from a machine with prod access: ssh databarn@<prod-host> bash -s < infra/prod/diagnose_price_tests.sh
set -euo pipefail

C=$(docker ps -qf label=com.docker.compose.service=dbt | head -1)
[ -n "$C" ] || { echo "dbt container not found"; exit 1; }

q() {
  docker exec -i "$C" sh -c 'curl -sS -u "$DBT_CLICKHOUSE_USER:$DBT_CLICKHOUSE_PASSWORD" \
    "http://$DBT_CLICKHOUSE_HOST:$DBT_CLICKHOUSE_PORT/?default_format=PrettyCompactMonoBlock" \
    --data-binary @-'
}

echo "== vaults sharing a moo token"
q <<'SQL'
select network, earned_token, groupArray(id) ids, groupArray(token_decimals) decimals,
  groupArray(status) statuses, groupArray(created_at) created
from dbt.stg_beefy_api__vaults
group by network, earned_token
having count() > 1
SQL

echo "== scale test vaults: reported ppfs"
q <<'SQL'
select id, earned_token, token_decimals, price_per_full_share,
  toFloat64OrNull(price_per_full_share) / pow(10, token_decimals) as want_per_share
from dbt.stg_beefy_api__vaults
where id in ('venus-arb-wbtc', 'silov2-arbitrum-usdc-optima')
SQL

echo "== bifi-maxi-eol ppfs points, 2023-09..11"
q <<'SQL'
with p as (
  select product_id from dbt.int_databarn_v1__want_decimals
  where chain_id = 56 and product_address = '0xf7069e41c57ecc5f122093811d8c75bdb5f7c14e'
)
select 'vault_stats' src, toStartOfMonth(datetime) m, count() n, min(datetime) first, max(datetime) last
from dbt.stg_databarn_v1__vault_stats
where product_id in p and share_to_underlying_price > 0 and datetime between '2023-09-01' and '2023-11-30'
group by m
union all
select 'share_rate_feed', toStartOfMonth(r.datetime) m, count(), min(r.datetime), max(r.datetime)
from dbt.stg_databarn_v1__prices_raw r
inner join dbt.stg_databarn_v1__import_states s
  on r.price_feed_id = s.price_feed_id and s.import_type = 'product:share-rate'
where s.product_id in p and r.price > 0 and r.datetime between '2023-09-01' and '2023-11-30'
group by m
order by src, m
SQL

echo "== oldBIFI want price points, 2023-09..11"
q <<'SQL'
select toStartOfMonth(date_time) m, count() n, min(date_time) first, max(date_time) last
from dbt.int_beefy_oracle_price
where beefy_price_oracle_key = 'oldBIFI' and date_time between '2023-09-01' and '2023-11-30'
group by m order by m
SQL

echo "== bifi-maxi-eol share prices, 2023-09..11"
q <<'SQL'
select toStartOfMonth(date_time) m, price_source, count() n, min(date_time) first, max(date_time) last
from dbt.int_product_share_price
where chain_id = 56 and product_address = '0xf7069e41c57ecc5f122093811d8c75bdb5f7c14e'
  and date_time between '2023-09-01' and '2023-11-30'
group by m, price_source order by m
SQL
