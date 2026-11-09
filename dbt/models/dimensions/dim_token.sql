{{
  config(
    materialized='table',
    engine='MergeTree',
    tags=['tokens'],
    order_by=['chain_id', 'representation_address'],
  )
}}

-- Dimension table: Token reference data, one row per (chain_id, representation_address).
-- Sources, in priority order: API token list, vault want tokens (vault configs, retired vaults), vault share tokens
-- (int_product_keys, retired vaults, and the CLM managers and classic vaults the subgraph indexes but the API does not
-- list), vaults and CLM managers envio knows and nothing lists (no asset), CLM subgraph tokens (priced only when their
-- symbol is an oracle key).
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
  inner join {{ ref('dim_chain') }} chain_dim
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
  inner join {{ ref('dim_chain') }} chain_dim
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

-- Retired addresses the token list prices with an oracle are tokens, not vault shares: bridged mooBIFI, beS.
listed_retired_tokens as (
  select chain_id, representation_address
  from api_tokens
  where beefy_price_oracle_key in (
      select oracle_id from {{ ref('stg_beefy_db__price_oracles') }} where not endsWith(oracle_id, '-rp')
    )
    and (chain_id, representation_address) in (
      select chain_id, product_address from {{ ref('int_databarn_v1__retired_vaults') }}
    )
),

-- Vault share tokens: the product contract is the share token, always 18 decimals.
-- token_kind is 'beefy_' || product_type (beefy_classic_vault, beefy_clm_manager, beefy_reward_pool).
-- A boost receipt is worth the vault share it stakes, so it takes that share's asset.
-- A CLM manager's own oracle prices its share, and a reward pool share is worth its underlying token (unlike a
-- classic vault's oracle, which prices the want). It is declared here because the API token list is not a reliable
-- source: it omits most reward pools and some managers, and beefy_api___tokens is keyed on (chain, id), so
-- managers whose LPs share an id collapse into one row.
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
  inner join {{ ref('dim_product') }} p
    on pk.chain_id = p.chain_id
    and pk.product_address = p.product_address
  where pk.share_token_symbol is not null
    and (pk.chain_id, pk.product_address) not in (select chain_id, representation_address from listed_retired_tokens)

  union all

  -- Retired vaults whose moo symbol is not unique are not in the rows above.
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
  where (r.chain_id, r.product_address) not in (select chain_id, representation_address from listed_retired_tokens)

  union all

  select
    pk.chain_id as chain_id,
    pk.product_address as representation_address,
    cast(pk.product_address as Nullable(String)) as erc20_address,
    false as is_native,
    cast(pk.beefy_key as String) as symbol,
    cast(pk.beefy_key as String) as name,
    cast(18 as Int64) as decimals,
    'beefy_classic_boost' as token_kind,
    cast(null as Nullable(String)) as beefy_price_oracle_key,
    3 as source_priority
  from {{ ref('int_product_keys') }} pk
  where pk.boost_staked_product_address is not null

  union all

  -- CLM managers and classic vaults the CLM subgraph indexes but the API does not list: deployed and not listed
  -- yet (or never), or delisted. No oracle: priced by the CLM subgraph (int_price_series_map).
  -- Once the API lists one, the rows above take over (a CLM then maps to its own oracle).
  select
    chain_dim.chain_id as chain_id,
    sg.product_address as representation_address,
    cast(sg.product_address as Nullable(String)) as erc20_address,
    false as is_native,
    cast(ifNull(sg_tokens.symbol, '') as String) as symbol,
    cast(ifNull(sg_tokens.name, '') as String) as name,
    cast(18 as Int64) as decimals,
    sg.token_kind as token_kind,
    cast(null as Nullable(String)) as beefy_price_oracle_key,
    3 as source_priority
  from (
    select chain_key, assumeNotNull(clm_address) as product_address, 'beefy_clm_manager' as token_kind
    from {{ ref('stg_beefy_clm_subgraph__clms') }}
    where clm_address is not null
    union all
    select chain_key, assumeNotNull(classic_address) as product_address, 'beefy_classic_vault' as token_kind
    from {{ ref('stg_beefy_clm_subgraph__classics') }}
    where classic_address is not null
  ) sg
  inner join {{ ref('dim_chain') }} chain_dim
    on sg.chain_key = chain_dim.beefy_key
  left join (
    select chain_key, token_address, any(symbol) as symbol, any(name) as name
    from {{ ref('stg_beefy_clm_subgraph__tokens') }}
    group by chain_key, token_address
  ) sg_tokens
    on sg.chain_key = sg_tokens.chain_key
    and sg.product_address = sg_tokens.token_address
  where (chain_dim.chain_id, sg.product_address) not in (select chain_id, product_address from {{ ref('int_product_keys') }})
    and (chain_dim.chain_id, sg.product_address) not in (select chain_id, representation_address from listed_retired_tokens)
),

-- Vaults and CLM managers that Beefy's factories deployed (envio follows the factories) but the API never listed:
-- test vaults (mooTest), duplicates of listed vaults, deployments left unused. No price source, so no asset; the
-- token_kind ('unlisted_classic_vault', 'unlisted_clm_manager') tells a consumer that the contract is known and not a
-- mapping gap. Once the API or the CLM subgraph lists one, the rows above replace it.
unlisted_tokens as (
  select
    e.network_id as chain_id,
    e.address as representation_address,
    cast(e.address as Nullable(String)) as erc20_address,
    false as is_native,
    cast(ifNull(tok.symbol, '') as String) as symbol,
    cast(ifNull(tok.name, '') as String) as name,
    cast(ifNull(tok.decimals, 18) as Int64) as decimals,
    e.token_kind as token_kind,
    cast(null as Nullable(String)) as beefy_price_oracle_key,
    3 as source_priority
  from (
    select toInt64(network_id) as network_id, address, share_token_id, 'unlisted_classic_vault' as token_kind
    from {{ ref('stg_envio__classic_vault') }}
    union all
    select toInt64(network_id), address, share_token_id, 'unlisted_clm_manager'
    from {{ ref('stg_envio__clm_manager') }}
  ) e
  left join {{ ref('stg_envio__token') }} tok
    on e.share_token_id = tok.id
  where e.address != ''
    and (e.network_id, e.address) not in (select chain_id, product_address from {{ ref('int_product_keys') }})
    and (e.network_id, e.address) not in (select chain_id, representation_address from share_tokens)
    and (e.network_id, e.address) not in (select chain_id, representation_address from listed_retired_tokens)
),

-- Tokens seen on-chain by the CLM subgraph. Fills in what the configs lack, never overrides them.
-- A symbol that is also an oracle key takes that oracle: /prices is keyed by symbol.
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
    cast(tokens.symbol as Nullable(String)) as beefy_price_oracle_key,
    4 as source_priority
  from {{ ref('stg_beefy_clm_subgraph__tokens') }} tokens
  inner join {{ ref('dim_chain') }} chain_dim
    on tokens.chain_key = chain_dim.beefy_key
),

unioned as (
  select * from api_tokens
  union all
  select * from want_tokens
  union all
  select * from share_tokens
  union all
  select * from unlisted_tokens
  union all
  select * from subgraph_tokens
),

-- Classic vault shares follow the vault rule (oracle price * ppfs), so a token list or symbol oracle never prices
-- them: mooBIFI and mooSiloSonicUSDCe drift from it.
classic_share_tokens as (
  select chain_id, product_address as representation_address
  from {{ ref('int_product_keys') }}
  where beefy_share_price_oracle_key is null
    and (chain_id, product_address) not in (select chain_id, representation_address from listed_retired_tokens)
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
      )
      and not (
        u.source_priority in (1, 4)
        and (u.chain_id, u.representation_address) in (select chain_id, representation_address from classic_share_tokens)
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
),

resolved as (
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
),

-- Boost receipts and the reward pool receipts the CLM subgraph knows, which include pools gone from the API.
-- One receipt is worth one staked token. The subgraph lists some boosts too, hence union distinct.
staked_tokens as (
  select chain_id, product_address as token_address, assumeNotNull(boost_staked_product_address) as staked_address
  from {{ ref('int_product_keys') }}
  where boost_staked_product_address is not null

  union distinct

  select
    chain_dim.chain_id as chain_id,
    {{ evm_address('arrayJoin(clms.reward_pool_tokens_order)') }} as token_address,
    {{ evm_address('clms.manager_token_address') }} as staked_address
  from {{ ref('stg_beefy_clm_subgraph__clms') }} clms
  inner join {{ ref('dim_chain') }} chain_dim
    on clms.chain_key = chain_dim.beefy_key

  union distinct

  select
    chain_dim.chain_id as chain_id,
    {{ evm_address('arrayJoin(classics.reward_pool_tokens_order)') }} as token_address,
    {{ evm_address('classics.vault_shares_token_address') }} as staked_address
  from {{ ref('stg_beefy_clm_subgraph__classics') }} classics
  inner join {{ ref('dim_chain') }} chain_dim
    on classics.chain_key = chain_dim.beefy_key
)

select
  r.chain_id as chain_id,
  r.representation_address as representation_address,
  r.erc20_address as erc20_address,
  r.is_native as is_native,
  r.symbol as symbol,
  r.name as name,
  r.decimals as decimals,
  if(r.token_kind = 'erc20' and r.asset_id is null and staked.asset_id is not null, 'beefy_reward_pool', r.token_kind) as token_kind,
  multiIf(
    r.token_kind = 'beefy_classic_boost', staked.asset_id,
    r.asset_id is null, staked.asset_id,
    r.asset_id
  ) as asset_id
from resolved r
left join staked_tokens st
  on r.chain_id = st.chain_id
  and r.representation_address = st.token_address
left join resolved staked
  on st.chain_id = staked.chain_id
  and st.staked_address = staked.representation_address
