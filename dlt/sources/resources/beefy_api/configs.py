from __future__ import annotations
from typing import Any, AsyncIterator, Dict, List

import dlt
from lib.fetch import fetch_url_json_dict, fetch_url_json_list


def _stringify_total_supply(item: Dict[str, Any]) -> Dict[str, Any]:
    # prevent crashes where python tries to convert the total supply to an Int and it's too large
    if "totalSupply" in item:
        item["totalSupply"] = str(item["totalSupply"])
    return item


async def _load_json_list(url: str) -> List[Dict[str, Any]]:
    return [item async for item in fetch_url_json_list(url)]


async def get_beefy_api_vaults_resource() -> Any:
    items = [_stringify_total_supply(item) for item in await _load_json_list("https://api.beefy.finance/vaults")]

    @dlt.resource(
        name="vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_vaults() -> AsyncIterator[Dict[str, Any]]:
        for item in items:
            yield item

    return beefy_vaults()


async def get_beefy_api_gov_vaults_resource() -> Any:
    items = [_stringify_total_supply(item) for item in await _load_json_list("https://api.beefy.finance/gov-vaults")]

    @dlt.resource(
        name="gov_vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_gov_vaults() -> AsyncIterator[Dict[str, Any]]:
        for item in items:
            yield item

    return beefy_gov_vaults()


async def get_beefy_api_boosts_resource() -> Any:
    items = await _load_json_list("https://api.beefy.finance/boosts")

    @dlt.resource(
        name="boosts",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_boosts() -> AsyncIterator[Dict[str, Any]]:
        for item in items:
            yield item

    return beefy_boosts()


async def get_beefy_api_clm_vaults_resource() -> Any:
    items: List[Dict[str, Any]] = []
    for item in await _load_json_list("https://api.beefy.finance/clm-vaults"):
        _stringify_total_supply(item)
        if "vault" in item and "totalSupply" in item["vault"]:
            item["vault"]["totalSupply"] = str(item["vault"]["totalSupply"])
        if "pool" in item and "totalSupply" in item["pool"]:
            item["pool"]["totalSupply"] = str(item["pool"]["totalSupply"])
        items.append(item)

    @dlt.resource(
        name="clm_vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        columns={
            "feeTier": {"data_type": "text"},
        },
    )
    async def beefy_clm_vaults() -> AsyncIterator[Dict[str, Any]]:
        for item in items:
            yield item

    return beefy_clm_vaults()


async def get_beefy_api_cow_vaults_resource() -> Any:
    items = [_stringify_total_supply(item) for item in await _load_json_list("https://api.beefy.finance/cow-vaults")]

    @dlt.resource(
        name="cow_vaults",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_cow_vaults() -> AsyncIterator[Dict[str, Any]]:
        for item in items:
            yield item

    return beefy_cow_vaults()


async def get_beefy_api_tokens_resource() -> Any:
    payload, _ = await fetch_url_json_dict("https://api.beefy.finance/tokens")
    items: List[Dict[str, Any]] = []
    for chain_id, tokens in payload.items():
        for _token_id, token_data in tokens.items():
            token_data["chainId"] = chain_id
            items.append(token_data)

    @dlt.resource(
        name="tokens",
        primary_key=["chainId", "id"],
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    )
    async def beefy_tokens() -> AsyncIterator[Dict[str, Any]]:
        for item in items:
            yield item

    return beefy_tokens()
