{{
  config(
    materialized='view',
    tags=['data_quality', 'debug']
  )
}}

-- Data Quality Model: vaults whose want decimals disagree between sources.
-- implied_want_decimals comes from the median ppfs (close to 1, scaled as if the want had 18 decimals). Not the
-- first ppfs: it can be off by the whole scale before the first deposit. It is an estimate, as a vault can
-- earn its way to a ppfs of 18, so only a gap of 2 or more counts.

with median_ppfs as (
  select product_id, quantile(0.5)(toFloat64(share_to_underlying_price)) as median_ppfs
  from {{ ref('stg_databarn_v1__vault_stats') }}
  where share_to_underlying_price > 0
  group by product_id
),

compared as (
  select
    d.chain_id as chain_id,
    d.product_address as product_address,
    pc.beefy_key as beefy_key,
    d.want_address as want_address,
    d.want_decimals as want_decimals,
    d.databarn_want_decimals as databarn_want_decimals,
    d.subgraph_want_decimals as subgraph_want_decimals,
    if(pc.beefy_key != '', toNullable(t.decimals), null) as token_want_decimals,
    if(f.median_ppfs > 0, toNullable(toInt64(18 - round(log10(f.median_ppfs)))), null) as implied_want_decimals,
    if(f.median_ppfs > 0, toNullable(f.median_ppfs), null) as median_ppfs
  from {{ ref('int_databarn_v1__want_decimals') }} d
  left join median_ppfs f
    on d.product_id = f.product_id
  left join {{ ref('product_classic') }} pc
    on d.chain_id = pc.chain_id
    and d.product_address = pc.vault_address
  left join {{ ref('token') }} t
    on pc.chain_id = t.chain_id
    and pc.underlying_token_representation_address = t.representation_address
)

select *
from compared
where ifNull(databarn_want_decimals != want_decimals, false)
  or ifNull(token_want_decimals != want_decimals, false)
  or ifNull(abs(implied_want_decimals - want_decimals) >= 2, false)
