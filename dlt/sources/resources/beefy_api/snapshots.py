from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict

import dlt
from lib.convert import get_int_like
from lib.fetch import fetch_url_json_dict


async def get_beefy_api_apy_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/apy")

    @dlt.resource(
        name="apy",
        primary_key=["etag", "vault_id"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "apy": {"data_type": "double"},
        },
    )
    async def beefy_apy() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for vault_id, vault_apy in payload.items():
            yield {
                "etag": etag_value,
                "vault_id": vault_id,
                "apy": str(vault_apy),
                "date_time": now,
            }

    return beefy_apy()


async def get_beefy_api_lps_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/lps")

    @dlt.resource(
        name="lps",
        primary_key=["etag", "vault_id"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "lps": {"data_type": "double"},
        },
    )
    async def beefy_lps() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for vault_id, vault_lps in payload.items():
            yield {
                "etag": etag_value,
                "vault_id": vault_id,
                "lps": str(vault_lps),
                "date_time": now,
            }

    return beefy_lps()


async def get_beefy_api_prices_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/prices")

    @dlt.resource(
        name="prices",
        primary_key=["etag", "token_symbol"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "price": {"data_type": "double"},
        },
    )
    async def beefy_prices() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for token_symbol, price in payload.items():
            yield {
                "etag": etag_value,
                "token_symbol": token_symbol,
                "price": str(price),
                "date_time": now,
            }

    return beefy_prices()


async def get_beefy_api_lps_breakdown_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/lps/breakdown")

    @dlt.resource(
        name="lps_breakdown",
        primary_key=["etag", "vault_id"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "price": {"data_type": "double"},
            "total_supply": {"data_type": "decimal"},
            "underlying_liquidity": {"data_type": "decimal"},
            "underlying_price": {"data_type": "decimal"},
        },
    )
    async def beefy_lps_breakdown() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for vault_id, breakdown in payload.items():
            yield {
                "etag": etag_value,
                "vault_id": str(vault_id),
                "date_time": now,
                **breakdown,
            }

    return beefy_lps_breakdown()


async def get_beefy_api_apy_breakdown_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/apy/breakdown")

    @dlt.resource(
        name="apy_breakdown",
        primary_key=["etag", "vault_id"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "compoundings_per_year": {"data_type": "bigint"},
            "beefy_performance_fee": {"data_type": "double"},
            "lp_fee": {"data_type": "double"},
            "total_apy": {"data_type": "double"},
            "vault_apr": {"data_type": "double"},
            "trading_apr": {"data_type": "double"},
            "clm_apr": {"data_type": "double"},
            "reward_pool_apr": {"data_type": "double"},
            "reward_pool_trading_apr": {"data_type": "double"},
        },
    )
    async def beefy_apy_breakdown() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for vault_id, breakdown in payload.items():
            breakdown = {k: str(v) for k, v in breakdown.items()}
            yield {
                "etag": etag_value,
                "vault_id": str(vault_id),
                "date_time": now,
                **breakdown,
            }

    return beefy_apy_breakdown()


async def get_beefy_api_tvl_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/tvl")

    @dlt.resource(
        name="tvl",
        primary_key=["etag", "network_id", "vault_id"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "tvl": {"data_type": "double"},
        },
    )
    async def beefy_tvl() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for network_id, vaults in payload.items():
            for vault_id, tvl in vaults.items():
                yield {
                    "etag": etag_value,
                    "network_id": str(network_id),
                    "vault_id": str(vault_id),
                    "tvl": float(tvl) if tvl is not None else None,
                    "date_time": now,
                }

    return beefy_tvl()


async def get_beefy_api_mootokenprices_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/mootokenprices")

    @dlt.resource(
        name="mootokenprices",
        primary_key=["etag", "chain_id", "moo_token_symbol"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "price": {"data_type": "double"},
        },
    )
    async def beefy_mootokenprices() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for chain_id, vaults in payload.items():
            for moo_token_symbol, price in vaults.items():
                yield {
                    "etag": etag_value,
                    "chain_id": str(chain_id),
                    "moo_token_symbol": str(moo_token_symbol),
                    "price": float(price) if price is not None else None,
                    "date_time": now,
                }

    return beefy_mootokenprices()


async def get_beefy_api_treasury_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/treasury")

    @dlt.resource(
        name="treasury",
        primary_key=["etag", "chain_id", "wallet_address", "token_address"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "token_decimals": {"data_type": "decimal"},
            "token_price": {"data_type": "double"},
            "usd_value": {"data_type": "decimal"},
        },
    )
    async def beefy_treasury() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for chain_id, wallets in payload.items():
            for wallet_address, wallet_data in wallets.items():
                for token_address_or_native, token_data in wallet_data.get("balances", {}).items():
                    row = {k: str(v) for k, v in token_data.items() if v is not None}
                    row["usdValue"] = get_int_like(row, "usdValue")
                    yield {
                        "etag": etag_value,
                        "chain_id": str(chain_id),
                        "wallet_address": str(wallet_address),
                        "wallet_name": wallet_data.get("name", ""),
                        "token_address": str(token_address_or_native),
                        "date_time": now,
                        **row,
                    }

    return beefy_treasury()


async def get_beefy_api_treasury_mm_resource() -> Any:
    payload, etag = await fetch_url_json_dict("https://api.beefy.finance/treasury/mm")

    @dlt.resource(
        name="treasury_mm",
        primary_key=["etag", "mm_id", "exchange_name", "token_symbol"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "price": {"data_type": "double"},
            "usd_value": {"data_type": "decimal"},
            "balance": {"data_type": "decimal"},
        },
    )
    async def beefy_treasury_mm() -> AsyncIterator[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        etag_value = etag or now.isoformat()
        for mm_id, exchanges in payload.items():
            for exchange_name, tokens in exchanges.items():
                for token_symbol, token_data in tokens.items():
                    row = {k: str(v) for k, v in token_data.items() if v is not None}
                    row["usdValue"] = get_int_like(row, "usdValue")
                    yield {
                        "etag": etag_value,
                        "mm_id": str(mm_id),
                        "exchange_name": str(exchange_name),
                        "token_symbol": str(token_symbol),
                        "date_time": now,
                        **row,
                    }

    return beefy_treasury_mm()
