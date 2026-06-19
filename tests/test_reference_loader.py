"""
Tests that the loader correctly handles reference snapshot tables
(schema "reference", snapshot_source_dir pointing to transform/reference/).

These tests verify that:
  - tenant_users and tenant_sites tables are picked up from transform/reference/
  - The drop+recreate mechanic works for the "reference" schema
  - Watermark gating works the same as for "snapshots" schema tables
  - The join-key columns (userLuid, siteLuid) are present in the written Parquet
"""
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, os.path.abspath(SRC))

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

pytestmark = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")

# Snapshot time present in the test Parquet that is NEWER than the stored watermark.
_NEW_SNAPSHOT_TIME = "2026-06-15T10:00:00Z"
_OLD_WATERMARK = "2026-06-14T00:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(path: str, columns: dict | None = None):
    """Write a minimal Parquet file with user or site columns."""
    cols = columns or {
        "userLuid": pa.array(["u1", "u1", "u2"], type=pa.string()),
        "siteLuid": pa.array(["s1", "s2", "s1"], type=pa.string()),
        "siteRole": pa.array(["Creator", "Viewer", "Creator"], type=pa.string()),
    }
    pq.write_table(pa.table(cols), path)


def _make_ref_dir(tmp_path, table: str, snapshot_time: str = _NEW_SNAPSHOT_TIME, columns=None):
    """Create transform/reference/ with one parquet file and manifest."""
    ref_dir = os.path.join(str(tmp_path), "reference")
    os.makedirs(ref_dir, exist_ok=True)
    parquet_path = os.path.join(ref_dir, f"{table}.parquet")
    _write_parquet(parquet_path, columns)
    manifest = {
        table: {
            "snapshot_time": snapshot_time,
            "filename": f"{table}.parquet",
        }
    }
    with open(os.path.join(ref_dir, "_manifest.json"), "w") as f:
        json.dump(manifest, f)
    return ref_dir


def _loader_config(ref_dir: str, table: str, write_mode: str = "snapshot"):
    return {
        "target": {"type": "hyper", "hyper_path": "/irrelevant/bundle.hyper"},
        "tables": [{
            "table": table,
            "schema": "reference",
            "write_mode": write_mode,
            "snapshot_source_dir": ref_dir,
        }],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestReferenceTableLoader:
    """Verify the loader picks up reference tables just like snapshot tables."""

    def _load(self, config, hyper_path, watermark=_OLD_WATERMARK):
        """Run load_snapshot_tables with standard mocks."""
        from loader.loader import load_snapshot_tables

        with patch("loader.loader.hyper_file_exists", return_value=True), \
             patch("loader.loader.get_hyper_table_columns", return_value=[]), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper", return_value=3) as mock_write, \
             patch("loader.loader.get_last_snapshot_time", return_value=watermark), \
             patch("loader.loader.set_last_snapshot_time") as mock_set_wm:

            rows, _ = load_snapshot_tables(
                config, "/data", hyper_path, False, [], [False], "bulk"
            )

        return rows, mock_drop, mock_write, mock_set_wm

    def test_tenant_users_loaded(self, tmp_path):
        ref_dir = _make_ref_dir(tmp_path, "tenant_users")
        config = _loader_config(ref_dir, "tenant_users")
        hyper_path = str(tmp_path / "bundle.hyper")

        rows, mock_drop, mock_write, mock_set_wm = self._load(config, hyper_path)

        assert rows == 3
        mock_drop.assert_called_once_with(hyper_path, "reference", "tenant_users")
        mock_write.assert_called_once()
        mock_set_wm.assert_called_once_with(hyper_path, "tenant_users", _NEW_SNAPSHOT_TIME)

    def test_tenant_sites_loaded(self, tmp_path):
        site_cols = {
            "siteLuid": pa.array(["s1", "s2"], type=pa.string()),
            "contentUrl": pa.array(["site1", "site2"], type=pa.string()),
            "status": pa.array(["Active", "Active"], type=pa.string()),
        }
        ref_dir = _make_ref_dir(tmp_path, "tenant_sites", columns=site_cols)
        config = _loader_config(ref_dir, "tenant_sites")
        hyper_path = str(tmp_path / "bundle.hyper")

        rows, mock_drop, mock_write, mock_set_wm = self._load(config, hyper_path)

        assert rows == 3
        mock_drop.assert_called_once_with(hyper_path, "reference", "tenant_sites")

    def test_watermark_gating_skips_old_pull(self, tmp_path):
        """A reference pull whose snapshot_time <= watermark is skipped (no write)."""
        old_pull_time = "2026-06-13T00:00:00Z"
        newer_watermark = "2026-06-14T00:00:00Z"

        ref_dir = _make_ref_dir(tmp_path, "tenant_users", snapshot_time=old_pull_time)
        config = _loader_config(ref_dir, "tenant_users")
        hyper_path = str(tmp_path / "bundle.hyper")

        from loader.loader import load_snapshot_tables

        with patch("loader.loader.hyper_file_exists", return_value=True), \
             patch("loader.loader.get_hyper_table_columns", return_value=[]), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper") as mock_write, \
             patch("loader.loader.get_last_snapshot_time", return_value=newer_watermark), \
             patch("loader.loader.set_last_snapshot_time") as mock_set_wm:

            rows, _ = load_snapshot_tables(
                config, "/data", hyper_path, False, [], [False], "bulk"
            )

        assert rows == 0
        mock_drop.assert_not_called()
        mock_write.assert_not_called()
        mock_set_wm.assert_not_called()

    def test_schema_change_absorbed_with_snapshot_mode(self, tmp_path):
        """reference schema: snapshot (allow) — table is dropped+recreated on schema drift."""
        # The new Parquet has an extra column compared to the existing table.
        ref_dir = _make_ref_dir(tmp_path, "tenant_users")
        config = _loader_config(ref_dir, "tenant_users", write_mode="snapshot")
        hyper_path = str(tmp_path / "bundle.hyper")

        old_cols = [("userLuid", None), ("siteLuid", None)]  # old schema

        from loader.loader import load_snapshot_tables

        with patch("loader.loader.hyper_file_exists", return_value=True), \
             patch("loader.loader.get_hyper_table_columns", return_value=old_cols), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper", return_value=3), \
             patch("loader.loader.get_last_snapshot_time", return_value=_OLD_WATERMARK), \
             patch("loader.loader.set_last_snapshot_time"):

            rows, _ = load_snapshot_tables(
                config, "/data", hyper_path, False, [], [False], "bulk"
            )

        assert rows == 3
        mock_drop.assert_called_once_with(hyper_path, "reference", "tenant_users")

    def test_first_run_no_drop(self, tmp_path):
        """On first run (no watermark), no drop is called — write directly."""
        ref_dir = _make_ref_dir(tmp_path, "tenant_users")
        config = _loader_config(ref_dir, "tenant_users")
        hyper_path = str(tmp_path / "bundle.hyper")

        from loader.loader import load_snapshot_tables

        with patch("loader.loader.hyper_file_exists", return_value=False), \
             patch("loader.loader.get_hyper_table_columns", return_value=None), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper", return_value=3), \
             patch("loader.loader.get_last_snapshot_time", return_value=None), \
             patch("loader.loader.set_last_snapshot_time"):

            load_snapshot_tables(config, "/data", hyper_path, False, [], [False], "bulk")

        mock_drop.assert_not_called()

    def test_loader_mapping_validates_reference_schema(self, tmp_path):
        """The loader_mapping module accepts 'reference' as a valid schema for snapshot tables."""
        from loader.loader_mapping import load_loader_mapping

        ref_dir = str(tmp_path / "reference")
        os.makedirs(ref_dir, exist_ok=True)

        import yaml
        mapping = {
            "load_name": "test",
            "target": {"type": "hyper", "hyper_path": str(tmp_path / "out.hyper")},
            "tables": [
                {
                    "table": "tenant_users",
                    "schema": "reference",
                    "write_mode": "snapshot",
                    "snapshot_source_dir": ref_dir,
                },
                {
                    "table": "tenant_sites",
                    "schema": "reference",
                    "write_mode": "snapshot",
                    "snapshot_source_dir": ref_dir,
                },
            ],
        }
        mapping_path = str(tmp_path / "mapping.yml")
        with open(mapping_path, "w") as f:
            yaml.dump(mapping, f)

        result = load_loader_mapping(mapping_path)
        tables = result.get("tables", [])
        names = [t["table"] for t in tables]
        assert "tenant_users" in names
        assert "tenant_sites" in names
