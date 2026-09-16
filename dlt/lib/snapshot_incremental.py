"""Honor Incremental.duplicate_cursor_warning_threshold on instances.

dlt 1.30 checks the ClassVar (200) and Incremental.merge()/copy() drop instance
overrides. Snapshot tables (prices/tvls/apys/tvl_by_chain) set a higher value
on the resource incremental; this keeps that value across copies.
"""

from __future__ import annotations

from typing import Any

from dlt.common import logger
from dlt.extract.incremental import Incremental

_THRESHOLD_ATTR = "duplicate_cursor_warning_threshold"
_original_merge = Incremental.merge


def _check_duplicate_cursor_threshold(
    self: Incremental[Any], initial_hash_count: int, final_hash_count: int
) -> None:
    threshold = self.duplicate_cursor_warning_threshold
    if initial_hash_count <= threshold < final_hash_count:
        logger.warning(
            f"Large number of records ({final_hash_count}) sharing the same value of cursor"
            f" field '{self.cursor_path}' on resource '{self.resource_name}'. This can happen"
            " if the cursor field has a low resolution (e.g., only stores dates without"
            " times), causing many records to share the same cursor value. Consider using a"
            " cursor column with higher resolution to reduce the deduplication state size."
        )


def _merge(self: Incremental[Any], other: Incremental[Any]) -> Incremental[Any]:
    merged = _original_merge(self, other)
    for src in (other, self):
        if _THRESHOLD_ATTR in src.__dict__:
            setattr(merged, _THRESHOLD_ATTR, src.__dict__[_THRESHOLD_ATTR])
            break
    return merged


Incremental._check_duplicate_cursor_threshold = _check_duplicate_cursor_threshold
Incremental.merge = _merge
