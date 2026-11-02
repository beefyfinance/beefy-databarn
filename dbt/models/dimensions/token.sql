{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['dimension', 'tokens'],
    order_by=['chain_id', 'representation_address'],
  )
}}

-- Dimension table: Token reference data, one row per (chain_id, representation_address).
-- Sources, in priority order: API token list, vault want tokens (vault configs, retired vaults), vault share tokens
-- (int_product_keys, retired vaults), CLM subgraph tokens (no price mapping).
-- asset_id links the token to its price series (asset dimension); the same token on several chains shares one asset.
-- Price mapping: beefy_db oracle first (a CLM manager's own oracle, then token list oracle_id, then vault oracle_id,
-- only if the oracle exists in beefy_db), else for vault share tokens the share price series (mootokenprices, databarn v1 ppfs).
-- A classic vault want oracle (often the vault id) prices the want token, NOT the share token.
-- beefy_db price_oracles.tokens is not usable for this: it lists an LP's constituent tokens, without chain.

with api_tokens as (
  select
    chain_dim.chain_id as chain_id,
    {{ to_representation_evm_address('tokens.address') }} as representation_address,
    cast(if(tokens.type = 'erc20', tokens.address, null) as Nullable(String)) as erc20_address,
    ifNull(toBool(tokens.type = 'native'), false) as is_native,
    cast(tokens.symbol as String) as symbol,
    cast(tokens.name as String) as name,
    cast(tokens.decimals as Int64) as decimals,
    multiIf(tokens.type = 'native', 'native', tokens.oracle = 'lps', 'lp_token', 'erc20') as token_kind,
    cast(nullIf(tokens.oracle_id, '') as Nullable(String)) as beefy_price_oracle_key,
    1 as source_priority
  from {{ ref('stg_beefy_api__tokens') }} tokens
  inner join {{ ref('chain') }} chain_dim
    on tokens.chain_beefy_key = chain_dim.beefy_key
),

-- Vault want (deposit) tokens. A null token_address is a native-asset vault (ETH, AVAX, FTM...).
vault_wants as (
  select network, token_address, token, token_decimals, oracle, oracle_id
  from {{ ref('stg_beefy_api__vaults') }}
  where not is_gov_vault
  union all
  select network, token_address, token, toInt32(token_decimals), oracle, oracle_id
  from {{ ref('stg_beefy_api__clm_vaults') }}
  where not is_gov_vault
  union all
  select network, token_address, token, token_decimals, oracle, oracle_id
  from {{ ref('stg_beefy_api__gov_vaults') }}
),

want_tokens as (
  select
    chain_dim.chain_id as chain_id,
    {{ to_representation_evm_address('w.token_address') }} as representation_address,
    cast(nullIf(w.token_address, '') as Nullable(String)) as erc20_address,
    toBool(ifNull(w.token_address, '') = '') as is_native,
    cast(ifNull(w.token, 'Unknown') as String) as symbol,
    cast(ifNull(w.token, 'Unknown') as String) as name,
    cast(w.token_decimals as Int64) as decimals,
    multiIf(ifNull(w.token_address, '') = '', 'native', w.oracle = 'lps', 'lp_token', 'erc20') as token_kind,
    cast(nullIf(w.oracle_id, '') as Nullable(String)) as beefy_price_oracle_key,
    2 as source_priority
  from vault_wants w
  inner join {{ ref('chain') }} chain_dim
    on w.network = chain_dim.beefy_key

  union all

  select
    r.chain_id as chain_id,
    r.underlying_token_representation_address as representation_address,
    cast(r.want_address as Nullable(String)) as erc20_address,
    toBool(r.want_address is null) as is_native,
    cast(r.beefy_wants_price_oracle_key as String) as symbol,
    cast(r.beefy_wants_price_oracle_key as String) as name,
    cast(r.want_decimals as Int64) as decimals,
    multiIf(r.want_address is null, 'native', r.want_is_lp, 'lp_token', 'erc20') as token_kind,
    cast(r.beefy_wants_price_oracle_key as Nullable(String)) as beefy_price_oracle_key,
    2 as source_priority
  from {{ ref('int_databarn_v1__retired_vaults') }} r
),

-- Vault share tokens: the product contract is the share token, always 18 decimals.
-- token_kind is 'beefy_' || product_type (beefy_classic_vault, beefy_clm_manager, beefy_reward_pool).
-- Boost receipts have no share_token_symbol and are left out.
-- A CLM manager's own oracle prices its share (unlike a classic vault's, which prices the want). It is
-- declared here because the API token list is not a reliable source: it omits some managers, and
-- beefy_api___tokens is keyed on (chain, id), so managers whose LPs share an id collapse into one row.
share_tokens as (
  select
    pk.chain_id as chain_id,
    pk.product_address as representation_address,
    cast(pk.product_address as Nullable(String)) as erc20_address,
    false as is_native,
    cast(pk.share_token_symbol as String) as symbol,
    cast(pk.beefy_key as String) as name,
    cast(18 as Int64) as decimals,
    concat('beefy_', p.product_type) as token_kind,
    pk.beefy_share_price_oracle_key as beefy_price_oracle_key,
    3 as source_priority
  from {{ ref('int_product_keys') }} pk
  inner join {{ ref('product') }} p
    on pk.chain_id = p.chain_id
    and pk.product_address = p.product_address
  where pk.share_token_symbol is not null

  union all

  -- Retired vaults have no share oracle: their share price comes from databarn v1.
  select
    r.chain_id as chain_id,
    r.product_address as representation_address,
    cast(r.product_address as Nullable(String)) as erc20_address,
    false as is_native,
    cast(r.share_token_symbol as String) as symbol,
    cast(r.beefy_key as String) as name,
    cast(18 as Int64) as decimals,
    'beefy_classic_vault' as token_kind,
    cast(null as Nullable(String)) as beefy_price_oracle_key,
    3 as source_priority
  from {{ ref('int_databarn_v1__retired_vaults') }} r
),

-- Tokens seen on-chain by the CLM subgraph. Fills in what the configs lack, never overrides them.
subgraph_tokens as (
  select
    chain_dim.chain_id as chain_id,
    {{ to_representation_evm_address('tokens.token_address') }} as representation_address,
    cast(tokens.token_address as Nullable(String)) as erc20_address,
    false as is_native,
    cast(tokens.symbol as String) as symbol,
    cast(tokens.name as String) as name,
    cast(tokens.decimals as Int64) as decimals,
    'erc20' as token_kind,
    cast(null as Nullable(String)) as beefy_price_oracle_key,
    4 as source_priority
  from {{ ref('stg_beefy_clm_subgraph__tokens') }} tokens
  inner join {{ ref('chain') }} chain_dim
    on tokens.chain_key = chain_dim.beefy_key
),

unioned as (
  select * from api_tokens
  union all
  select * from want_tokens
  union all
  select * from share_tokens
  union all
  select * from subgraph_tokens
),

-- Only oracles that exist in beefy_db (and are not '-rp' duplicates) have a price series.
-- A share token used as a want (e.g. a CLM share in a reward pool) never takes the vault's want oracle:
-- that oracle prices the underlying, not the share.
priced as (
  select
    u.*,
    if(
      u.beefy_price_oracle_key in (
        select oracle_id from {{ ref('stg_beefy_db__price_oracles') }} where not endsWith(oracle_id, '-rp')
      )
      and not (
        u.source_priority = 2
        and (u.chain_id, u.representation_address) in (select chain_id, representation_address from share_tokens)
      ),
      u.beefy_price_oracle_key,
      null
    ) as priced_oracle_key
  from unioned u
),

tokens as (
  select
    chain_id,
    representation_address,
    argMin(erc20_address, source_priority) as erc20_address,
    argMin(is_native, source_priority) as is_native,
    argMin(symbol, source_priority) as symbol,
    argMin(name, source_priority) as name,
    argMin(decimals, source_priority) as decimals,
    -- A share token is a share even when the token list also lists it (e.g. mooBIFI).
    if(max(source_priority = 3), argMinIf(token_kind, source_priority, source_priority = 3), argMin(token_kind, source_priority)) as token_kind,
    -- A token's own share oracle (CLM managers) wins. Otherwise several vaults can declare different oracles
    -- for one want: lowest priority, then alphabetical.
    nullIf(argMinIf(ifNull(priced_oracle_key, ''), (source_priority != 3, source_priority, ifNull(priced_oracle_key, '')), priced_oracle_key is not null), '') as beefy_price_oracle_key
  from priced
  group by chain_id, representation_address
)

select
  t.chain_id as chain_id,
  t.representation_address as representation_address,
  t.erc20_address as erc20_address,
  t.is_native as is_native,
  t.symbol as symbol,
  t.name as name,
  t.decimals as decimals,
  t.token_kind as token_kind,
  cast(multiIf(
    t.beefy_price_oracle_key is not null, {{ asset_id_from_beefy_price_oracle_id('o.id') }},
    startsWith(t.token_kind, 'beefy_'), {{ asset_id_from_share_token('t.chain_id', 't.representation_address') }},
    null
  ) as Nullable(UInt64)) as asset_id
from tokens t
left join {{ ref('stg_beefy_db__price_oracles') }} o
  on t.beefy_price_oracle_key = o.oracle_id
