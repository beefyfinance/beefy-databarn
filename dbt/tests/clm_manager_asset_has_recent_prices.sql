-- Fails when a CLM manager's token maps to an asset with no recent price: the indexer stores those events
-- as MISSING and shows USD 0. Returns the managers to fix.
-- New managers get a grace period: the first price lands a median of 17 hours after creation and about
-- 8% take over 48 hours, so declared_creation_date must be older than clm_price_grace_hours.

{% set max_age_hours = var('clm_price_max_age_hours', 48) %}
{% set grace_hours = var('clm_price_grace_hours', 168) %}

with recently_priced as (
  select distinct asset_id
  from {{ ref('asset_price_5m') }}
  where date_time >= now() - interval {{ max_age_hours }} hour
)

select
  t.chain_id as chain_id,
  t.representation_address as representation_address,
  p.beefy_key as beefy_key,
  p.is_active as is_active,
  p.declared_creation_date as declared_creation_date,
  t.asset_id as asset_id,
  a.asset_source as asset_source,
  a.beefy_price_oracle_key as beefy_price_oracle_key
from {{ ref('token') }} t
inner join {{ ref('product') }} p
  on t.chain_id = p.chain_id
  and t.representation_address = p.product_address
  and p.product_type = 'clm_manager'
left join {{ ref('asset') }} a
  on t.asset_id = a.asset_id
where t.token_kind = 'beefy_clm_manager'
  and ifNull(t.asset_id, 0) not in (select asset_id from recently_priced)
  and p.declared_creation_date < now() - interval {{ grace_hours }} hour
