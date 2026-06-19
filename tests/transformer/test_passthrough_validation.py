"""
Tests for the snapshot union pipeline in the transformer passthrough stage.

Covers:
  - select_latest_per_site_per_partition (reader helper):
      * single file per entity
      * multiple files per entity, multiple sites → newest hour kept per site
      * multiple part files at same hour within one site → all returned
      * files with no siteLuid fall into a single group
  - _run_passthrough_pipeline (latest_per_partition):
      * two sites, same entity → rows from both sites unioned, siteLuid column present
      * one site, multiple part files at the same hour → all parts unioned
      * per-site latest-hour selection (older hour dropped, newer kept)
      * corrupt part skipped with warning; remaining valid parts still unioned
      * all parts for an entity corrupt → RuntimeError
  - _run_passthrough_pipeline (select=all):
      * all files for each entity unioned; corrupt files skipped with warning
"""

import io
import json
import os
import shutil
import sys
import tempfile
import unittest

SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, os.path.abspath(SRC))

import pyarrow as pa
import pyarrow.parquet as pq

from storage.local import LocalStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(path: str, rows: list, schema: pa.Schema = None) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if rows:
        table = pa.Table.from_pylist(rows, schema=schema)
    else:
        table = pa.table({}) if schema is None else pa.table(
            {f.name: pa.array([], type=f.type) for f in schema}
        )
    pq.write_table(table, path)


def _corrupt_file(path: str) -> None:
    """Overwrite a file with invalid bytes (missing Parquet footer magic)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"PAR1" + b"x" * 100)  # missing PAR1 footer


def _read_parquet(path: str) -> pa.Table:
    return pq.read_table(path)


def _make_storage(base_dir: str) -> LocalStorage:
    return LocalStorage(base_dir)


# ---------------------------------------------------------------------------
# Tests for select_latest_per_site_per_partition (reader helper)
# ---------------------------------------------------------------------------

class TestSelectLatestPerSitePerPartition(unittest.TestCase):

    def _call(self, files):
        from transformer.reader import select_latest_per_site_per_partition
        return select_latest_per_site_per_partition(files, partition_key="entityType", site_key="siteLuid")

    def test_single_file(self):
        files = [
            "pod=p1/siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
        ]
        result = self._call(files)
        self.assertIn("users", result)
        paths, max_date = result["users"]
        self.assertEqual(len(paths), 1)
        self.assertEqual(max_date, (2026, 6, 8, 2))

    def test_two_sites_same_entity_both_included(self):
        files = [
            "pod=p1/siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            "pod=p2/siteLuid=bbb/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
        ]
        result = self._call(files)
        paths, max_date = result["users"]
        self.assertEqual(len(paths), 2)
        luid_in_paths = {"aaa", "bbb"}
        for p in paths:
            self.assertTrue(any(l in p for l in luid_in_paths))

    def test_multiple_parts_at_same_hour_all_included(self):
        files = [
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-1.parquet",
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-2.parquet",
        ]
        result = self._call(files)
        paths, _ = result["users"]
        self.assertEqual(len(paths), 3)

    def test_older_hour_dropped_newer_kept(self):
        files = [
            "siteLuid=aaa/y=2026/m=06/d=08/h=01/entityType=users/part-0.parquet",
            "siteLuid=aaa/y=2026/m=06/d=08/h=04/entityType=users/part-0.parquet",
        ]
        result = self._call(files)
        paths, max_date = result["users"]
        self.assertEqual(len(paths), 1)
        self.assertIn("h=04", paths[0])
        self.assertEqual(max_date, (2026, 6, 8, 4))

    def test_per_site_newest_selected_independently(self):
        # site aaa has h=04, site bbb has h=02 — both are kept at their own newest
        files = [
            "siteLuid=aaa/y=2026/m=06/d=08/h=04/entityType=users/part-0.parquet",
            "siteLuid=aaa/y=2026/m=06/d=08/h=01/entityType=users/part-0.parquet",
            "siteLuid=bbb/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
        ]
        result = self._call(files)
        paths, max_date = result["users"]
        # aaa h=04 + bbb h=02 = 2 files
        self.assertEqual(len(paths), 2)
        self.assertEqual(max_date, (2026, 6, 8, 4))
        hours = {p.split("h=")[1].split("/")[0] for p in paths}
        self.assertEqual(hours, {"04", "02"})

    def test_no_site_luid_single_group(self):
        files = [
            "entityType=users/y=2026/m=06/d=08/h=02/part-0.parquet",
            "entityType=users/y=2026/m=06/d=08/h=04/part-0.parquet",
        ]
        result = self._call(files)
        # All files with no siteLuid fall into a single unnamed group → newest hour only
        paths, max_date = result["users"]
        self.assertEqual(len(paths), 1)
        self.assertIn("h=04", paths[0])

    def test_multiple_entity_types(self):
        files = [
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=workbooks/part-0.parquet",
        ]
        result = self._call(files)
        self.assertIn("users", result)
        self.assertIn("workbooks", result)

    def test_files_without_entity_type_excluded(self):
        files = [
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/no_entity_here/part-0.parquet",
        ]
        result = self._call(files)
        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# Tests for _run_passthrough_pipeline (latest_per_partition)
# ---------------------------------------------------------------------------

class TestPassthroughLatestPerPartition(unittest.TestCase):

    def setUp(self):
        self.base_dir = tempfile.mkdtemp()
        self.out_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.base_dir, ignore_errors=True)
        shutil.rmtree(self.out_dir, ignore_errors=True)

    def _make_file(self, rel_path: str, rows: list) -> str:
        full_path = os.path.join(self.base_dir, rel_path)
        _write_parquet(full_path, rows)
        return full_path

    def _corrupt(self, rel_path: str) -> str:
        full_path = os.path.join(self.base_dir, rel_path)
        _corrupt_file(full_path)
        return full_path

    def _run(self, select_mode: str = "latest_per_partition"):
        from transformer.pipeline import _run_passthrough_pipeline
        storage = _make_storage(self.base_dir)
        config = {
            "output_filename": "snapshot",
            "output": {"format": "parquet", "target_path": ""},
            "select": select_mode,
            "partition_field": "entityType",
            "passthrough": True,
            "split_by": "entityType",
            "inputs": [{"source": {"folder_pattern": "*", "file_pattern": "*.parquet", "recursive": True}}],
        }
        return _run_passthrough_pipeline("op1", storage, config, self.out_dir, "test")

    def test_two_sites_rows_unioned(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "u1", "name": "Alice"}],
        )
        self._make_file(
            "siteLuid=bbb/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "u2", "name": "Bob"}, {"id": "u3", "name": "Carol"}],
        )
        written, fmt = self._run()
        out = os.path.join(self.out_dir, "users.parquet")
        self.assertTrue(os.path.exists(out))
        tbl = _read_parquet(out)
        self.assertEqual(tbl.num_rows, 3)
        self.assertIn("siteLuid", tbl.schema.names)
        site_luids = set(tbl.column("siteLuid").to_pylist())
        self.assertEqual(site_luids, {"aaa", "bbb"})

    def test_siteLuid_column_added(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "u1"}],
        )
        self._run()
        tbl = _read_parquet(os.path.join(self.out_dir, "users.parquet"))
        self.assertIn("siteLuid", tbl.schema.names)
        self.assertEqual(tbl.column("siteLuid").to_pylist(), ["aaa"])

    def test_multiple_parts_same_site_all_unioned(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=groups/part-0.parquet",
            [{"gid": "g1"}],
        )
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=groups/part-1.parquet",
            [{"gid": "g2"}, {"gid": "g3"}],
        )
        self._run()
        tbl = _read_parquet(os.path.join(self.out_dir, "groups.parquet"))
        self.assertEqual(tbl.num_rows, 3)

    def test_older_hour_dropped_newer_hour_kept(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=01/entityType=users/part-0.parquet",
            [{"id": "old"}],
        )
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=04/entityType=users/part-0.parquet",
            [{"id": "new"}],
        )
        self._run()
        tbl = _read_parquet(os.path.join(self.out_dir, "users.parquet"))
        self.assertEqual(tbl.num_rows, 1)
        self.assertEqual(tbl.column("id").to_pylist(), ["new"])

    def test_per_site_latest_hour_selected_independently(self):
        # site aaa latest = h=04, site bbb latest = h=02 → both included
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=01/entityType=users/part-0.parquet",
            [{"id": "aaa_old"}],
        )
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=04/entityType=users/part-0.parquet",
            [{"id": "aaa_new"}],
        )
        self._make_file(
            "siteLuid=bbb/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "bbb"}],
        )
        self._run()
        tbl = _read_parquet(os.path.join(self.out_dir, "users.parquet"))
        ids = set(tbl.column("id").to_pylist())
        self.assertEqual(ids, {"aaa_new", "bbb"})
        self.assertNotIn("aaa_old", ids)

    def test_corrupt_part_skipped_valid_parts_still_written(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "good"}],
        )
        self._corrupt(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-1.parquet",
        )
        with self.assertLogs("transformer.pipeline", level="WARNING") as cm:
            written, _ = self._run()
        out = os.path.join(self.out_dir, "users.parquet")
        self.assertTrue(os.path.exists(out))
        tbl = _read_parquet(out)
        self.assertEqual(tbl.num_rows, 1)
        self.assertIn("integrity", " ".join(cm.output).lower())

    def test_all_parts_corrupt_raises(self):
        self._corrupt(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
        )
        self._corrupt(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-1.parquet",
        )
        with self.assertLogs("transformer.pipeline", level="ERROR"):
            with self.assertRaises(RuntimeError) as ctx:
                self._run()
        self.assertIn("users", str(ctx.exception))

    def test_manifest_written_with_snapshot_time(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "u1"}],
        )
        self._run()
        manifest_path = os.path.join(self.out_dir, "_manifest.json")
        self.assertTrue(os.path.exists(manifest_path))
        with open(manifest_path) as f:
            manifest = json.load(f)
        self.assertIn("users", manifest)
        self.assertEqual(manifest["users"]["filename"], "users.parquet")
        self.assertEqual(manifest["users"]["snapshot_time"], "2026-06-08T02:00:00Z")

    def test_manifest_snapshot_time_is_max_across_sites(self):
        # site aaa is at h=04, site bbb at h=02 → max = h=04
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=04/entityType=users/part-0.parquet",
            [{"id": "a"}],
        )
        self._make_file(
            "siteLuid=bbb/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "b"}],
        )
        self._run()
        with open(os.path.join(self.out_dir, "_manifest.json")) as f:
            manifest = json.load(f)
        self.assertEqual(manifest["users"]["snapshot_time"], "2026-06-08T04:00:00Z")

    def test_multiple_entity_types_written_independently(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "u1"}],
        )
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=workbooks/part-0.parquet",
            [{"wb_id": "w1"}, {"wb_id": "w2"}],
        )
        self._run()
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, "users.parquet")))
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, "workbooks.parquet")))
        tbl_u = _read_parquet(os.path.join(self.out_dir, "users.parquet"))
        tbl_w = _read_parquet(os.path.join(self.out_dir, "workbooks.parquet"))
        self.assertEqual(tbl_u.num_rows, 1)
        self.assertEqual(tbl_w.num_rows, 2)


# ---------------------------------------------------------------------------
# Tests for _run_passthrough_pipeline (select=all)
# ---------------------------------------------------------------------------

class TestPassthroughSelectAll(unittest.TestCase):

    def setUp(self):
        self.base_dir = tempfile.mkdtemp()
        self.out_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.base_dir, ignore_errors=True)
        shutil.rmtree(self.out_dir, ignore_errors=True)

    def _make_file(self, rel_path: str, rows: list) -> str:
        full_path = os.path.join(self.base_dir, rel_path)
        _write_parquet(full_path, rows)
        return full_path

    def _corrupt(self, rel_path: str) -> str:
        full_path = os.path.join(self.base_dir, rel_path)
        _corrupt_file(full_path)
        return full_path

    def _run(self):
        from transformer.pipeline import _run_passthrough_pipeline
        storage = _make_storage(self.base_dir)
        config = {
            "output_filename": "snapshot",
            "output": {"format": "parquet", "target_path": ""},
            "select": "all",
            "partition_field": "entityType",
            "passthrough": True,
            "split_by": "entityType",
            "inputs": [{"source": {"folder_pattern": "*", "file_pattern": "*.parquet", "recursive": True}}],
        }
        return _run_passthrough_pipeline("op1", storage, config, self.out_dir, "test")

    def test_all_files_unioned(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "u1"}],
        )
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=01/entityType=users/part-0.parquet",
            [{"id": "u_old"}],
        )
        self._run()
        tbl = _read_parquet(os.path.join(self.out_dir, "users.parquet"))
        self.assertEqual(tbl.num_rows, 2)

    def test_corrupt_file_skipped_with_warning(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "good"}],
        )
        self._corrupt(
            "siteLuid=aaa/y=2026/m=06/d=08/h=01/entityType=users/part-0.parquet",
        )
        with self.assertLogs("transformer.pipeline", level="WARNING") as cm:
            self._run()
        tbl = _read_parquet(os.path.join(self.out_dir, "users.parquet"))
        self.assertEqual(tbl.num_rows, 1)
        self.assertIn("integrity", " ".join(cm.output).lower())

    def test_siteLuid_column_added(self):
        self._make_file(
            "siteLuid=aaa/y=2026/m=06/d=08/h=02/entityType=users/part-0.parquet",
            [{"id": "u1"}],
        )
        self._run()
        tbl = _read_parquet(os.path.join(self.out_dir, "users.parquet"))
        self.assertIn("siteLuid", tbl.schema.names)


# ---------------------------------------------------------------------------
# Pure-unit tests for group_files_by_partition_newest_first (unchanged)
# ---------------------------------------------------------------------------

class TestGroupFilesNewestFirst(unittest.TestCase):

    def test_single_file_per_entity(self):
        from transformer.reader import group_files_by_partition_newest_first
        files = ["entityType=users/y=2026/m=06/d=07/h=12/part-0.parquet"]
        result = group_files_by_partition_newest_first(files, "entityType")
        self.assertIn("users", result)
        self.assertEqual(len(result["users"]), 1)

    def test_multiple_files_sorted_newest_first(self):
        from transformer.reader import group_files_by_partition_newest_first
        files = [
            "entityType=users/y=2026/m=06/d=07/h=06/part-0.parquet",
            "entityType=users/y=2026/m=06/d=07/h=14/part-0.parquet",
            "entityType=users/y=2026/m=06/d=07/h=10/part-0.parquet",
        ]
        result = group_files_by_partition_newest_first(files, "entityType")
        paths = result["users"]
        hours = [int(p.split("h=")[1].split("/")[0]) for p in paths]
        self.assertEqual(hours, sorted(hours, reverse=True))

    def test_multiple_entity_types_grouped(self):
        from transformer.reader import group_files_by_partition_newest_first
        files = [
            "entityType=users/y=2026/m=06/d=07/h=12/part-0.parquet",
            "entityType=datasources/y=2026/m=06/d=07/h=12/part-0.parquet",
            "entityType=users/y=2026/m=06/d=07/h=10/part-0.parquet",
        ]
        result = group_files_by_partition_newest_first(files, "entityType")
        self.assertEqual(set(result.keys()), {"users", "datasources"})
        self.assertEqual(len(result["users"]), 2)
        self.assertEqual(len(result["datasources"]), 1)


if __name__ == "__main__":
    unittest.main()
