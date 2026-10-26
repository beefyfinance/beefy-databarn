"""Keep GzipFile alive on Python 3.13+ write buffers.

CPython 3.13 (gh-129726) stores only a weakref from
``gzip._WriteBufferStream`` back to ``GzipFile``. If GC collects the
GzipFile while its ``BufferedWriter`` still flushes, ``write()`` raises
``RuntimeError: lost gzip_file`` and the pipeline exits 1.

Hold a strong ref on the stream so the file object outlives pending
flushes. Imported for side effects from ``lib.cli``.
"""
from __future__ import annotations

import gzip
import sys

_PATCHED = False


def apply() -> None:
    global _PATCHED
    if _PATCHED:
        return
    _PATCHED = True
    if sys.version_info < (3, 13):
        return
    stream_cls = getattr(gzip, "_WriteBufferStream", None)
    if stream_cls is None:
        return
    original_init = stream_cls.__init__

    def __init__(self, gzip_file):  # noqa: N807
        original_init(self, gzip_file)
        self._gzip_file_keepalive = gzip_file

    stream_cls.__init__ = __init__  # type: ignore[method-assign]


apply()
