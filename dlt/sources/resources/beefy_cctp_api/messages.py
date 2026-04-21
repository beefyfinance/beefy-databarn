from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Optional

import dlt
from dlt.destinations.adapters import clickhouse_adapter

from lib.fetch import fetch_url_json_dict_with_params

CCTP_API_BASE_URL = "https://cctp-relay.beefy.com"


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    # API spec says "date-time" (ISO 8601); handle trailing Z.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_dt_param(value: datetime) -> str:
    """Format datetime for API query params (strict RFC3339 in UTC, no micros)."""
    dt = value.astimezone(timezone.utc).replace(microsecond=0)
    # Use trailing Z (not +00:00) to satisfy API validator
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


async def get_beefy_cctp_api_messages_resource() -> Any:
    url = f"{CCTP_API_BASE_URL}/v1/messages"

    # Stable primary key based on source log identity.
    pk = ["srcNetworkId", "srcTxHash", "srcLogIndex"]

    columns = {
        "srcNetworkId": {"data_type": "text", "primary_key": True, "nullable": False, "codec": "LZ4"},
        "srcTxHash": {"data_type": "text", "primary_key": True, "nullable": False, "codec": "LZ4"},
        "srcLogIndex": {"data_type": "bigint", "primary_key": True, "nullable": False, "codec": "LZ4"},
        "updatedAt": {"data_type": "timestamp", "nullable": False, "codec": "ZSTD(3)"},
        "createdAt": {"data_type": "timestamp", "nullable": False, "codec": "ZSTD(3)"},
        # Remaining fields are mostly identifiers / bigints represented as strings; keep as text unless numeric is clear.
        "srcBlockNumber": {"data_type": "text", "codec": "LZ4"},
        "srcBlockHash": {"data_type": "text", "codec": "LZ4"},
        "srcBlockTimestamp": {"data_type": "text", "codec": "LZ4"},
        "srcMessage": {"data_type": "text", "codec": "ZSTD(3)"},
        "srcSender": {"data_type": "text", "codec": "LZ4"},
        "srcBurnToken": {"data_type": "text", "codec": "LZ4"},
        "srcBurnAmount": {"data_type": "text", "codec": "LZ4"},
        "attestationMessage": {"data_type": "text", "codec": "ZSTD(3)"},
        "attestation": {"data_type": "text", "codec": "ZSTD(3)"},
        "attestationNonce": {"data_type": "text", "codec": "LZ4"},
        "attestationVersion": {"data_type": "bigint", "codec": "LZ4"},
        "attestationStatus": {"data_type": "text", "codec": "LZ4"},
        "lifecycleState": {"data_type": "text", "codec": "LZ4"},
        "dstNetworkId": {"data_type": "text", "codec": "LZ4"},
        "dstReceiver": {"data_type": "text", "codec": "LZ4"},
        "dstRecipient": {"data_type": "text", "codec": "LZ4"},
        "dstTxHash": {"data_type": "text", "codec": "LZ4"},
        "dstTxCaller": {"data_type": "text", "codec": "LZ4"},
        "dstLogIndex": {"data_type": "bigint", "codec": "LZ4"},
        "dstBlockNumber": {"data_type": "text", "codec": "LZ4"},
        "dstBlockTimestamp": {"data_type": "text", "codec": "LZ4"},
        "dstRelayAttempts": {"data_type": "bigint", "codec": "LZ4"},
        "dstZapSuccess": {"data_type": "bool", "codec": "LZ4"},
        "dstAmountIn": {"data_type": "text", "codec": "LZ4"},
        "dstRefundedAmount": {"data_type": "text", "codec": "LZ4"},
        "dstRecoveredAmount": {"data_type": "text", "codec": "LZ4"},
        "errorCode": {"data_type": "text", "codec": "LZ4"},
        "errorMessage": {"data_type": "text", "codec": "ZSTD(3)"},
    }

    @dlt.resource(
        name="messages",
        primary_key=pk,
        write_disposition="append",
        columns={k: {kk: vv for kk, vv in v.items() if kk != "codec"} for k, v in columns.items()},
    )
    async def messages(
        updated_at: dlt.sources.incremental[Any] = dlt.sources.incremental(
            "updatedAt",
            initial_value=None,
            primary_key=pk,
            last_value_func=max,
            row_order="asc",
        ),
    ) -> AsyncIterator[Dict[str, Any]]:
        cursor: Optional[str] = None

        # Avoid missing records at the boundary: use >= and let PK + ReplacingMergeTree collapse duplicates.
        updated_at_gte: Optional[str]
        if updated_at.start_value is None:
            updated_at_gte = None
        elif isinstance(updated_at.start_value, datetime):
            updated_at_gte = _format_dt_param(updated_at.start_value)
        else:
            # Best-effort normalization if state is stored as a string
            s = str(updated_at.start_value)
            try:
                updated_at_gte = _format_dt_param(_parse_dt(s) or datetime.fromisoformat(s))
            except Exception:
                updated_at_gte = s

        while True:
            params: Dict[str, Any] = {"limit": 500}
            if cursor:
                params["cursor"] = cursor
            if updated_at_gte:
                params["updatedAtGte"] = updated_at_gte

            payload, _ = await fetch_url_json_dict_with_params(url, params=params)
            rows = payload.get("messages", [])
            for row in rows:
                if isinstance(row, dict):
                    row["createdAt"] = _parse_dt(row.get("createdAt"))
                    row["updatedAt"] = _parse_dt(row.get("updatedAt"))
                yield row

            cursor = payload.get("cursor")
            if not cursor:
                break

    resource = messages()
    resource = clickhouse_adapter(
        resource,
        table_engine_type="replacing_merge_tree",
        sort=pk,
        settings={"allow_nullable_key": False},
        codecs={name: c["codec"] for name, c in columns.items() if "codec" in c},
    )

    return resource

