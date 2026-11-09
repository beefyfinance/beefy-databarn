-- Fails when a classic vault share token maps to a beefy_db oracle instead of its share price series.
-- Its price must follow oracle price * ppfs, which a token list oracle does not.

select
  t.chain_id as chain_id,
  t.representation_address as representation_address,
  t.symbol as symbol,
  a.beefy_price_oracle_key as beefy_price_oracle_key
from {{ ref('dim_token') }} t
inner join {{ ref('dim_asset') }} a
  on t.asset_id = a.asset_id
where t.token_kind = 'beefy_classic_vault'
  and a.asset_source = 'beefy_db_oracle'
