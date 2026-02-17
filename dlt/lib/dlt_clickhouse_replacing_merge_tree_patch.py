"""
Monkey-patch dlt ClickHouse adapter to support ReplacingMergeTree until
dlt-hub/dlt#3366 is merged and released.

Apply by importing this module before any use of clickhouse_adapter:
    import lib.dlt_clickhouse_replacing_merge_tree_patch  # noqa: F401
"""

from dlt.destinations.impl.clickhouse import typing as ch_typing

# Add ReplacingMergeTree (PR #3366)
ch_typing.TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR["replacing_merge_tree"] = "ReplacingMergeTree"
ch_typing.TABLE_ENGINE_TYPES.add("replacing_merge_tree")
