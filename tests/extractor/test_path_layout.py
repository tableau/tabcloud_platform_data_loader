"""Tests for extractor path layout mapping and detection."""
import os
import tempfile
import unittest

from extractor.path_layout import (
    ExtractPathLayout,
    assert_layout_matches_disk,
    detect_layout_from_extract_root,
    file_present_for_layouts,
    map_api_to_local_relpath,
    normalize_slashes,
    other_layout,
)


EXAMPLE_DF = (
    "pod=10az/siteLuid=00000000-0000-0000-0000-000000000001"
    "/y=2025/m=11/d=30/h=02/eventType=vizql_http_request/part-0001.json"
)
EXAMPLE_SF = (
    "pod=10az/siteLuid=00000000-0000-0000-0000-000000000001"
    "/eventType=vizql_http_request/y=2025/m=11/d=30/h=02/part-0001.json"
)


class TestMapPaths(unittest.TestCase):
    def test_date_first_is_identity(self):
        self.assertEqual(
            map_api_to_local_relpath(EXAMPLE_DF, ExtractPathLayout.DATE_FIRST),
            normalize_slashes(EXAMPLE_DF),
        )

    def test_schema_first_reorder(self):
        self.assertEqual(
            map_api_to_local_relpath(EXAMPLE_DF, ExtractPathLayout.SCHEMA_FIRST),
            EXAMPLE_SF,
        )

    def test_backslash_input(self):
        win = EXAMPLE_DF.replace("/", "\\")
        self.assertEqual(
            map_api_to_local_relpath(win, ExtractPathLayout.SCHEMA_FIRST),
            EXAMPLE_SF,
        )

    def test_malformed_raises(self):
        with self.assertRaises(ValueError):
            map_api_to_local_relpath("a/b/c.txt", ExtractPathLayout.SCHEMA_FIRST)

    def test_other_layout(self):
        self.assertEqual(other_layout(ExtractPathLayout.DATE_FIRST), ExtractPathLayout.SCHEMA_FIRST)
        self.assertEqual(other_layout(ExtractPathLayout.SCHEMA_FIRST), ExtractPathLayout.DATE_FIRST)

    def test_from_cli(self):
        self.assertEqual(ExtractPathLayout.from_cli("date-first"), ExtractPathLayout.DATE_FIRST)
        self.assertEqual(ExtractPathLayout.from_cli("schema_first"), ExtractPathLayout.SCHEMA_FIRST)


class TestDetectAssert(unittest.TestCase):
    def test_empty_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(detect_layout_from_extract_root(d))

    def test_detect_date_first(self):
        with tempfile.TemporaryDirectory() as d:
            rel = EXAMPLE_DF
            full = os.path.join(d, *rel.split("/"))
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write("{}")
            self.assertEqual(detect_layout_from_extract_root(d), ExtractPathLayout.DATE_FIRST)

    def test_detect_schema_first(self):
        with tempfile.TemporaryDirectory() as d:
            rel = EXAMPLE_SF
            full = os.path.join(d, *rel.split("/"))
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write("{}")
            self.assertEqual(detect_layout_from_extract_root(d), ExtractPathLayout.SCHEMA_FIRST)

    def test_mixed_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            for sub in (EXAMPLE_DF, EXAMPLE_SF):
                full = os.path.join(d, *sub.split("/"))
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w", encoding="utf-8") as f:
                    f.write("{}")
            self.assertIsNone(detect_layout_from_extract_root(d))

    def test_assert_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            full = os.path.join(d, *EXAMPLE_DF.split("/"))
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write("{}")
            with self.assertRaises(ValueError):
                assert_layout_matches_disk(ExtractPathLayout.SCHEMA_FIRST, d)


class TestFilePresent(unittest.TestCase):
    def test_primary_hit(self):
        with tempfile.TemporaryDirectory() as d:
            p = map_api_to_local_relpath(EXAMPLE_DF, ExtractPathLayout.DATE_FIRST)
            full = os.path.join(d, p.replace("/", os.sep))
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write("{}")
            self.assertTrue(
                file_present_for_layouts(
                    d, EXAMPLE_DF, ExtractPathLayout.DATE_FIRST, check_alternate=True
                )
            )

    def test_alternate_hit(self):
        with tempfile.TemporaryDirectory() as d:
            p = map_api_to_local_relpath(EXAMPLE_DF, ExtractPathLayout.SCHEMA_FIRST)
            full = os.path.join(d, p.replace("/", os.sep))
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write("{}")
            # Writing schema-first, checking date-first with alternate
            self.assertTrue(
                file_present_for_layouts(
                    d, EXAMPLE_DF, ExtractPathLayout.DATE_FIRST, check_alternate=True
                )
            )


if __name__ == "__main__":
    unittest.main()
