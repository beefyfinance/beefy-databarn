#!/bin/bash
# Read-only: checks the new price test logic against prod before deploying it.
# Run from a machine with prod access: ssh databarn@<prod-host> bash -s < infra/prod/diagnose_price_tests.sh
set -euo pipefail

C=$(docker ps -qf label=com.docker.compose.service=dbt | head -1)
[ -n "$C" ] || { echo "dbt container not found"; exit 1; }

q() {
  docker exec -i "$C" sh -c 'curl -sS -u "$DBT_CLICKHOUSE_USER:$DBT_CLICKHOUSE_PASSWORD" \
    "http://$DBT_CLICKHOUSE_HOST:$DBT_CLICKHOUSE_PORT/?default_format=PrettyCompactMonoBlock" \
    --data-binary @-'
}

echo "== is_listed: vault counts, and the scale test vaults"
q <<'SQL'
with latest as (select max(_dlt_load_id) as id from dlt.beefy_api___vaults)
select id, network, _dlt_load_id, _dlt_load_id = (select id from latest) as is_listed
from dlt.beefy_api___vaults FINAL
where id in ('venus-arb-wbtc', 'silov2-arbitrum-usdc-optima', 'venus-zksync-wbtc')
union all
select concat('total listed / all'), toString(countIf(_dlt_load_id = (select id from latest))), toString(count()), null
from dlt.beefy_api___vaults FINAL
SQL

echo "== new databarn_ppfs_reaches_share_price (expect no rows)"
q <<'SQL'
-- Fails when a databarn v1 vault has ppfs and a want price in a month, but int_product_share_price has no price
-- for it that month: the join on want decimals, product keys or the want oracle lost it.
-- The model only prices ppfs with a want price at most 1 day older, so a ppfs day only counts with a want price
-- that day or the day before. A same-month match is not enough: oldBIFI prices start 2023-10-24, after the
-- October ppfs of bifi-maxi-eol. A want price later the same day than every ppfs point can still slip through.
-- Whole months, so this stays cheap over the full history. Not the current month: it can still be loading.

with ppfs as (
  select product_id, toDate(datetime) as day
  from `dbt`.`stg_databarn_v1__vault_stats`
  where share_to_underlying_price > 0
    and datetime >= '2021-08-01'
  group by product_id, day

  union distinct

  select assumeNotNull(s.product_id) as product_id, toDate(r.datetime) as day
  from `dbt`.`stg_databarn_v1__prices_raw` r
  inner join `dbt`.`stg_databarn_v1__import_states` s
    on r.price_feed_id = s.price_feed_id
    and s.import_type = 'product:share-rate'
  where r.price > 0
    and r.datetime >= '2021-08-01'
  group by product_id, day
),

-- A want price on day d covers ppfs on d and d + 1.
wants as (
  select distinct beefy_price_oracle_key, arrayJoin([toDate(date_time), toDate(date_time) + 1]) as day
  from `dbt`.`int_beefy_oracle_price`
),

shares as (
  select distinct chain_id, product_address, toStartOfMonth(date_time) as month
  from `dbt`.`int_product_share_price`
)

select distinct
  pk.chain_id as chain_id,
  pk.product_address as product_address,
  pk.beefy_key as beefy_key,
  toStartOfMonth(p.day) as month
from ppfs p
inner join `dbt`.`int_databarn_v1__want_decimals` wd
  on p.product_id = wd.product_id
inner join `dbt`.`int_product_keys` pk
  on wd.chain_id = pk.chain_id
  and wd.product_address = pk.product_address
inner join wants w
  on pk.beefy_wants_price_oracle_key = w.beefy_price_oracle_key
  and p.day = w.day
left anti join shares s
  on pk.chain_id = s.chain_id
  and pk.product_address = s.product_address
  and toStartOfMonth(p.day) = s.month
where p.day < toStartOfMonth(now())
SQL
