"""Beefy CLM subgraph entities to ingest, one table each."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Bytes and BigInt stay text: uint256 values overflow every dlt numeric type.
TIMESTAMP_FIELDS = frozenset({"timestamp", "roundedTimestamp", "blockTimestamp"})
INTEGER_FIELDS = frozenset({"blockNumber", "logIndex", "period"})


@dataclass(frozen=True)
class Entity:
    table: str
    query: str
    fields: tuple[str, ...]
    # Fields holding a GraphQL list, stored as a JSON string column.
    arrays: tuple[str, ...] = ()
    # Full refresh each run when None.
    cursor: Optional[str] = None
    where: str = ""


_TOKEN_REFS = ("underlyingToken0 { id }", "underlyingToken1 { id }")
_CLM_PRICES = (
    "token0ToNativePrice",
    "token1ToNativePrice",
    "outputToNativePrices",
    "rewardToNativePrices",
    "nativeToUSDPrice",
)
_PRICE_ARRAYS = ("outputToNativePrices", "rewardToNativePrices")
_CLASSIC_PRICES = (
    "underlyingToNativePrice",
    "underlyingBreakdownToNativePrices",
    "boostRewardToNativePrices",
    "rewardToNativePrices",
    "nativeToUSDPrice",
)
_CLASSIC_PRICE_ARRAYS = (
    "underlyingBreakdownToNativePrices",
    "boostRewardToNativePrices",
    "rewardToNativePrices",
)
_EVENT = ("id", "clm { id }", "createdWith { id }", "logIndex", "timestamp")
_CLM_RANGE = (
    "priceOfToken0InToken1",
    "priceRangeMin1",
    "priceRangeMax1",
    "totalUnderlyingAmount0",
    "totalUnderlyingAmount1",
    "underlyingMainAmount0",
    "underlyingMainAmount1",
    "underlyingAltAmount0",
    "underlyingAltAmount1",
    "totalCallFees",
    "totalBeefyFees",
    "totalStrategistFees",
)
_CLASSIC_SUPPLY = (
    "vaultSharesTotalSupply",
    "vaultUnderlyingTotalSupply",
    "vaultUnderlyingBreakdownBalances",
    "vaultUnderlyingBalance",
    "rewardPoolsTotalSupply",
    "erc4626AdaptersTotalSupply",
    "erc4626AdapterVaultSharesBalances",
)
_CLASSIC_SUPPLY_ARRAYS = (
    "vaultUnderlyingBreakdownBalances",
    "rewardPoolsTotalSupply",
    "erc4626AdaptersTotalSupply",
    "erc4626AdapterVaultSharesBalances",
)

ENTITIES: tuple[Entity, ...] = (
    # INITIALIZING rows have null token references, which fail non-null fields in the query.
    Entity(
        table="clms",
        query="clms",
        where='lifecycle_not: INITIALIZING',
        fields=(
            "id",
            "lifecycle",
            "manager { id }",
            "strategy { id }",
            "underlyingProtocolPool",
            "managerToken { id }",
            *_TOKEN_REFS,
            "outputTokensOrder",
            "rewardTokensOrder",
            "rewardPoolTokensOrder",
            "managerTotalSupply",
            "rewardPoolsTotalSupply",
            *_CLM_PRICES,
            *_CLM_RANGE,
        ),
        arrays=(
            "outputTokensOrder",
            "rewardTokensOrder",
            "rewardPoolTokensOrder",
            "rewardPoolsTotalSupply",
            *_PRICE_ARRAYS,
        ),
    ),
    # No strategy reference: it is null on a running sonic vault. Envio has ClassicVaultStrategy.
    Entity(
        table="classics",
        query="classics",
        where='lifecycle_not: INITIALIZING',
        fields=(
            "id",
            "lifecycle",
            "vault { id }",
            "underlyingPlatform",
            "vaultSharesToken { id }",
            "underlyingToken { id }",
            "underlyingBreakdownTokensOrder",
            "boostRewardTokensOrder",
            "rewardTokensOrder",
            "rewardPoolTokensOrder",
            "erc4626AdapterTokensOrder",
            *_CLASSIC_SUPPLY,
            *_CLASSIC_PRICES,
            "underlyingAmount",
            "totalCallFees",
            "totalBeefyFees",
            "totalStrategistFees",
        ),
        arrays=(
            "underlyingBreakdownTokensOrder",
            "boostRewardTokensOrder",
            "rewardTokensOrder",
            "rewardPoolTokensOrder",
            "erc4626AdapterTokensOrder",
            *_CLASSIC_SUPPLY_ARRAYS,
            *_CLASSIC_PRICE_ARRAYS,
        ),
    ),
    Entity(
        table="tokens",
        query="tokens",
        fields=("id", "symbol", "name", "decimals"),
    ),
    # Daily and weekly rows are hourly rollups. Rows are rewritten until their period ends,
    # so the cursor is the period start and the lag re-reads the open period.
    Entity(
        table="clm_snapshots",
        query="clmSnapshots",
        cursor="roundedTimestamp",
        where="period: 3600",
        fields=(
            "id",
            "clm { id }",
            "period",
            "roundedTimestamp",
            "timestamp",
            "managerTotalSupply",
            "rewardPoolsTotalSupply",
            *_CLM_PRICES,
            *_CLM_RANGE,
        ),
        arrays=("rewardPoolsTotalSupply", *_PRICE_ARRAYS),
    ),
    Entity(
        table="classic_snapshots",
        query="classicSnapshots",
        cursor="roundedTimestamp",
        where="period: 3600",
        fields=(
            "id",
            "classic { id }",
            "period",
            "roundedTimestamp",
            "timestamp",
            *_CLASSIC_SUPPLY,
            *_CLASSIC_PRICES,
            "underlyingAmount",
            "totalCallFees",
            "totalBeefyFees",
            "totalStrategistFees",
        ),
        arrays=(*_CLASSIC_SUPPLY_ARRAYS, *_CLASSIC_PRICE_ARRAYS),
    ),
    Entity(
        table="clm_position_interactions",
        query="clmPositionInteractions",
        cursor="timestamp",
        fields=(
            *_EVENT,
            "blockNumber",
            "investor { id }",
            "investorPosition { id }",
            "type",
            "managerBalance",
            "rewardPoolBalances",
            "totalBalance",
            "underlyingBalance0",
            "underlyingBalance1",
            "managerBalanceDelta",
            "rewardPoolBalancesDelta",
            "rewardBalancesDelta",
            "claimedRewardPool { id }",
            "underlyingBalance0Delta",
            "underlyingBalance1Delta",
            *_CLM_PRICES,
        ),
        arrays=(
            "rewardPoolBalances",
            "rewardPoolBalancesDelta",
            "rewardBalancesDelta",
            *_PRICE_ARRAYS,
        ),
    ),
    Entity(
        table="classic_position_interactions",
        query="classicPositionInteractions",
        cursor="timestamp",
        fields=(
            "id",
            "classic { id }",
            "createdWith { id }",
            "logIndex",
            "timestamp",
            "blockNumber",
            "investor { id }",
            "investorPosition { id }",
            "type",
            "vaultBalance",
            "boostBalance",
            "rewardPoolBalances",
            "erc4626AdapterBalances",
            "erc4626AdapterVaultSharesBalances",
            "totalBalance",
            "vaultSharesTotalSupply",
            "vaultUnderlyingTotalSupply",
            "vaultUnderlyingBreakdownBalances",
            "vaultUnderlyingAmount",
            "vaultUnderlyingBalance",
            "vaultBalanceDelta",
            "boostBalanceDelta",
            "boostRewardBalancesDelta",
            "rewardPoolBalancesDelta",
            "rewardBalancesDelta",
            "erc4626AdapterBalancesDelta",
            "erc4626AdapterVaultSharesBalancesDelta",
            *_CLASSIC_PRICES,
        ),
        arrays=(
            "rewardPoolBalances",
            "erc4626AdapterBalances",
            "erc4626AdapterVaultSharesBalances",
            "vaultUnderlyingBreakdownBalances",
            "boostRewardBalancesDelta",
            "rewardPoolBalancesDelta",
            "rewardBalancesDelta",
            "erc4626AdapterBalancesDelta",
            "erc4626AdapterVaultSharesBalancesDelta",
            *_CLASSIC_PRICE_ARRAYS,
        ),
    ),
    Entity(
        table="clm_harvest_events",
        query="clmHarvestEvents",
        cursor="timestamp",
        fields=(
            *_EVENT,
            "blockNumber",
            "strategy { id }",
            "underlyingAmount0",
            "underlyingAmount1",
            "compoundedAmount0",
            "compoundedAmount1",
            "collectedOutputAmounts",
            "managerTotalSupply",
            "rewardPoolsTotalSupply",
            *_CLM_PRICES,
        ),
        arrays=("collectedOutputAmounts", "rewardPoolsTotalSupply", *_PRICE_ARRAYS),
    ),
    Entity(
        table="clm_manager_collection_events",
        query="clmManagerCollectionEvents",
        cursor="timestamp",
        fields=(
            *_EVENT,
            "blockNumber",
            "strategy { id }",
            "underlyingMainAmount0",
            "underlyingMainAmount1",
            "underlyingAltAmount0",
            "underlyingAltAmount1",
            "underlyingAmount0",
            "underlyingAmount1",
            "collectedAmount0",
            "collectedAmount1",
            "collectedOutputAmounts",
            *_CLM_PRICES,
        ),
        arrays=("collectedOutputAmounts", *_PRICE_ARRAYS),
    ),
    Entity(
        table="clm_deposit_events",
        query="clmDepositEvents",
        cursor="timestamp",
        fields=(
            *_EVENT,
            "blockNumber",
            "investor { id }",
            "shares",
            "amount0",
            "amount1",
            "fee0",
            "fee1",
        ),
    ),
    Entity(
        table="clm_withdraw_events",
        query="clmWithdrawEvents",
        cursor="timestamp",
        fields=(*_EVENT, "blockNumber", "investor { id }", "shares", "amount0", "amount1"),
    ),
    # No blockNumber on this entity; join created_with to transactions.
    Entity(
        table="clm_strategy_tvl_events",
        query="clmStrategyTVLEvents",
        cursor="timestamp",
        fields=(*_EVENT, "strategy { id }", "underlyingAmount0", "underlyingAmount1"),
    ),
    Entity(
        table="transactions",
        query="transactions",
        cursor="blockTimestamp",
        fields=("id", "blockNumber", "blockTimestamp", "sender"),
    ),
)
