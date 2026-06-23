"""
Tests for path-traversal / containment guards in LocalStorage and _safe_join.

Covers:
  - write_bytes rejects absolute paths
  - write_bytes rejects '..' traversal segments
  - write_bytes rejects symlink escapes (realpath containment check)
  - exists returns False (no raise) for bad paths
  - read_file / read_bytes raise ValueError for bad paths
  - list_files with bad prefix returns [] (no raise)
  - Legitimate relative paths continue to work
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, os.path.abspath(SRC))

from storage.local import LocalStorage, _safe_join


class TestSafeJoin(unittest.TestCase):
    """Unit tests for the _safe_join helper independently."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_normal_relative_path_ok(self):
        result = _safe_join(self._tmp, "a/b/c.txt")
        real_base = os.path.realpath(self._tmp)
        self.assertTrue(result.startswith(real_base))

    def test_absolute_path_raises(self):
        with self.assertRaises(ValueError, msg="absolute path should be rejected"):
            _safe_join(self._tmp, "/etc/passwd")

    def test_dotdot_segment_raises(self):
        with self.assertRaises(ValueError, msg="'..' segment should be rejected"):
            _safe_join(self._tmp, "subdir/../../etc/passwd")

    def test_dotdot_at_root_raises(self):
        with self.assertRaises(ValueError):
            _safe_join(self._tmp, "../sibling")

    def test_dotdot_only_raises(self):
        with self.assertRaises(ValueError):
            _safe_join(self._tmp, "..")

    def test_windows_backslash_dotdot_raises(self):
        with self.assertRaises(ValueError):
            _safe_join(self._tmp, "subdir\\..\\..\\escape")


class TestLocalStoragePathTraversal(unittest.TestCase):
    """Integration tests: LocalStorage methods must reject escape paths."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._storage = LocalStorage(self._tmp)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    # -- write_bytes ----------------------------------------------------------

    def test_write_bytes_rejects_absolute_path(self):
        with self.assertRaises(ValueError):
            self._storage.write_bytes("/tmp/evil.txt", b"data")

    def test_write_bytes_rejects_dotdot_traversal(self):
        with self.assertRaises(ValueError):
            self._storage.write_bytes("../escape.txt", b"data")

    def test_write_bytes_rejects_nested_dotdot(self):
        with self.assertRaises(ValueError):
            self._storage.write_bytes("sub/../../escape.txt", b"data")

    def test_write_bytes_legitimate_path_works(self):
        rel = "y=2026/m=01/d=01/h=00/eventType=Login/part-0.ndjson"
        data = b'{"event":"Login"}\n'
        self._storage.write_bytes(rel, data)
        written = os.path.join(self._tmp, rel)
        self.assertTrue(os.path.isfile(written))
        with open(written, "rb") as fh:
            self.assertEqual(fh.read(), data)

    # -- exists ---------------------------------------------------------------

    def test_exists_returns_false_for_absolute_path(self):
        # Must not raise; returns False for disallowed paths.
        self.assertFalse(self._storage.exists("/etc/passwd"))

    def test_exists_returns_false_for_dotdot(self):
        self.assertFalse(self._storage.exists("../sibling_dir/file.txt"))

    # -- read_file / read_bytes -----------------------------------------------

    def test_read_file_rejects_absolute_path(self):
        with self.assertRaises(ValueError):
            self._storage.read_file("/etc/hostname")

    def test_read_bytes_rejects_dotdot(self):
        with self.assertRaises(ValueError):
            self._storage.read_bytes("../secret")

    # -- list_files -----------------------------------------------------------

    def test_list_files_bad_prefix_returns_empty(self):
        result = self._storage.list_files("../escape")
        self.assertEqual(result, [])

    def test_list_files_absolute_prefix_returns_empty(self):
        result = self._storage.list_files("/etc")
        self.assertEqual(result, [])

    # -- full_path / uri ------------------------------------------------------

    def test_full_path_rejects_dotdot(self):
        with self.assertRaises(ValueError):
            self._storage.full_path("../escape")

    def test_uri_rejects_absolute_path(self):
        with self.assertRaises(ValueError):
            self._storage.uri("/absolute/path")


class TestLocalStorageSymlinkEscape(unittest.TestCase):
    """Containment check catches symlink-based escapes (realpath validation)."""

    def setUp(self):
        self._outer = tempfile.mkdtemp()
        self._inner = os.path.join(self._outer, "storage")
        os.makedirs(self._inner)
        self._storage = LocalStorage(self._inner)
        # Create a symlink inside storage pointing to the outer (parent) dir.
        self._link = os.path.join(self._inner, "escape_link")
        try:
            os.symlink(self._outer, self._link)
            self._symlinks_supported = True
        except (OSError, NotImplementedError):
            self._symlinks_supported = False

    def tearDown(self):
        import shutil
        shutil.rmtree(self._outer, ignore_errors=True)

    def test_write_through_symlink_is_blocked(self):
        if not self._symlinks_supported:
            self.skipTest("symlinks not supported on this platform")
        # "escape_link" is a symlink to the outer directory; writing a file
        # through it would land outside the storage root.
        with self.assertRaises(ValueError):
            self._storage.write_bytes("escape_link/evil.txt", b"data")


if __name__ == "__main__":
    unittest.main()
