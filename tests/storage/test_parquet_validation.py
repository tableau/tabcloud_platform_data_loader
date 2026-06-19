"""
Tests for the Parquet integrity helpers in storage.base.

Covers:
  - is_valid_parquet_bytes: valid / truncated (no footer) / too-short / non-Parquet
  - is_valid_parquet_file: valid / truncated / missing file / unreadable
  - LocalStorage.write_bytes: atomic rename (no partial file visible on failure)
"""

import os
import sys
import tempfile
import unittest

SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, os.path.abspath(SRC))

from storage.base import is_valid_parquet_bytes, is_valid_parquet_file
from storage.local import LocalStorage


# ---------------------------------------------------------------------------
# Minimal synthetic Parquet bytes
# ---------------------------------------------------------------------------

MAGIC = b"PAR1"

def _valid_parquet(payload: bytes = b"x" * 100) -> bytes:
    """Return bytes that look like a valid Parquet file (magic header + footer)."""
    return MAGIC + payload + MAGIC


def _truncated_parquet(payload: bytes = b"x" * 100) -> bytes:
    """Valid header only — missing footer (simulates truncated download)."""
    return MAGIC + payload


def _non_parquet(payload: bytes = b"x" * 100) -> bytes:
    """Bytes with no Parquet magic at all."""
    return b"FAKE" + payload + b"FAKE"


# ---------------------------------------------------------------------------
# is_valid_parquet_bytes
# ---------------------------------------------------------------------------

class TestIsValidParquetBytes(unittest.TestCase):
    def test_valid(self):
        self.assertTrue(is_valid_parquet_bytes(_valid_parquet()))

    def test_too_short_empty(self):
        self.assertFalse(is_valid_parquet_bytes(b""))

    def test_too_short_less_than_8(self):
        self.assertFalse(is_valid_parquet_bytes(MAGIC + b"xx" + MAGIC[:2]))

    def test_truncated_no_footer(self):
        self.assertFalse(is_valid_parquet_bytes(_truncated_parquet()))

    def test_non_parquet_header(self):
        self.assertFalse(is_valid_parquet_bytes(_non_parquet()))

    def test_none_data(self):
        self.assertFalse(is_valid_parquet_bytes(None))  # type: ignore[arg-type]

    def test_only_magic_bytes(self):
        # Exactly 8 bytes: PAR1PAR1 — valid by the magic check
        self.assertTrue(is_valid_parquet_bytes(MAGIC + MAGIC))

    def test_valid_footer_wrong_header(self):
        data = b"NOPE" + b"x" * 100 + MAGIC
        self.assertFalse(is_valid_parquet_bytes(data))

    def test_valid_header_wrong_footer(self):
        data = MAGIC + b"x" * 100 + b"NOPE"
        self.assertFalse(is_valid_parquet_bytes(data))


# ---------------------------------------------------------------------------
# is_valid_parquet_file
# ---------------------------------------------------------------------------

class TestIsValidParquetFile(unittest.TestCase):
    def _write(self, data: bytes, suffix: str = ".parquet") -> str:
        fd, path = tempfile.mkstemp(suffix=suffix)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
        return path

    def test_valid_file(self):
        path = self._write(_valid_parquet())
        try:
            self.assertTrue(is_valid_parquet_file(path))
        finally:
            os.unlink(path)

    def test_truncated_file(self):
        path = self._write(_truncated_parquet())
        try:
            self.assertFalse(is_valid_parquet_file(path))
        finally:
            os.unlink(path)

    def test_non_parquet_file(self):
        path = self._write(_non_parquet())
        try:
            self.assertFalse(is_valid_parquet_file(path))
        finally:
            os.unlink(path)

    def test_missing_file(self):
        self.assertFalse(is_valid_parquet_file("/no/such/file/at/all.parquet"))

    def test_empty_file(self):
        path = self._write(b"")
        try:
            self.assertFalse(is_valid_parquet_file(path))
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# LocalStorage.write_bytes — atomic rename
# ---------------------------------------------------------------------------

class TestLocalStorageAtomicWrite(unittest.TestCase):
    def test_successful_write_leaves_no_temp_file(self):
        with tempfile.TemporaryDirectory() as base:
            storage = LocalStorage(base)
            rel = "sub/test.parquet"
            data = _valid_parquet()
            storage.write_bytes(rel, data)
            target = os.path.join(base, rel)
            self.assertTrue(os.path.exists(target))
            self.assertEqual(open(target, "rb").read(), data)
            # No .part sibling should remain.
            self.assertFalse(os.path.exists(target + ".part"))

    def test_failed_write_cleans_up_temp_file(self):
        """If the caller raises after write_bytes is entered, .part is removed."""
        with tempfile.TemporaryDirectory() as base:
            storage = LocalStorage(base)
            rel = "sub/fail.parquet"
            target = os.path.join(base, rel)
            tmp = target + ".part"
            os.makedirs(os.path.dirname(target), exist_ok=True)

            # Simulate a write error by patching os.replace to raise.
            original_replace = os.replace
            def _bad_replace(src, dst):
                raise OSError("simulated disk full")
            try:
                os.replace = _bad_replace
                with self.assertRaises(OSError):
                    storage.write_bytes(rel, _valid_parquet())
            finally:
                os.replace = original_replace

            # The target should NOT exist (write never completed).
            self.assertFalse(os.path.exists(target))
            # The .part file should also have been cleaned up.
            self.assertFalse(os.path.exists(tmp))

    def test_content_is_correct(self):
        with tempfile.TemporaryDirectory() as base:
            storage = LocalStorage(base)
            data = b"hello world"
            storage.write_bytes("a.bin", data)
            self.assertEqual(storage.read_bytes("a.bin"), data)


if __name__ == "__main__":
    unittest.main()
