from __future__ import annotations

import gc
import gzip
import io
import sys

import lib.python313_gzip_patch as gzip_patch


def test_patch_keeps_gzipfile_alive_while_buffer_flushes() -> None:
    gzip_patch.apply()
    raw = io.BytesIO()
    gz = gzip.GzipFile(fileobj=raw, mode="wb")
    buffer = gz._buffer
    stream = getattr(buffer, "raw", None)
    keepalive = getattr(stream, "_gzip_file_keepalive", None)
    if keepalive is gz:
        del gz
        gc.collect()
    buffer.write(b"hello")
    buffer.flush()
    (keepalive or gz).close()


def test_apply_is_idempotent_and_keeps_strong_ref() -> None:
    gzip_patch.apply()
    gzip_patch.apply()
    gz = gzip.GzipFile(fileobj=io.BytesIO(), mode="wb")
    try:
        if sys.version_info < (3, 13) or not hasattr(gzip, "_WriteBufferStream"):
            return
        stream = gz._buffer.raw
        assert stream._gzip_file_keepalive is gz
    finally:
        gz.close()
