"""
Tests for entity-snapshot integration.

Covers:
  - DatasetProfile construction and get_profile()
  - extract_segment_value_from_path() (generalized from extract_event_type_from_path)
  - path_layout.py with segment_key="entityType"
  - storage/presence.py with partition_key="entityType"
  - LogRetriever._select_latest_per_partition()
  - LogRetriever._cadence_default_window()
  - LogRetriever._check_delivery_gaps()
  - LogRetriever URL construction with SNAPSHOTS profile
  - transformer reader.select_latest_per_partition()
  - transformer reader.select_files_for_mapping()
  - load_state snapshot watermark helpers
  - loader_mapping snapshot validation
"""

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# Ensure src/ is on the path.
SRC = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, os.path.abspath(SRC))


# ---------------------------------------------------------------------------
# DatasetProfile
# ---------------------------------------------------------------------------

class TestDatasetProfile:
    def test_activitylog_defaults(self):
        from common.dataset_profile import ACTIVITYLOG
        assert ACTIVITYLOG.name == "activitylog"
        assert ACTIVITYLOG.resource == "activitylog"
        assert ACTIVITYLOG.filter_param == "eventType"
        assert ACTIVITYLOG.path_key == "eventType"
        assert not ACTIVITYLOG.is_site_only
        assert not ACTIVITYLOG.files_are_parquet

    def test_snapshots_defaults(self):
        from common.dataset_profile import SNAPSHOTS
        assert SNAPSHOTS.name == "snapshots"
        assert SNAPSHOTS.resource == "snapshots"
        assert SNAPSHOTS.filter_param == "entityType"
        assert SNAPSHOTS.path_key == "entityType"
        assert SNAPSHOTS.is_site_only
        assert SNAPSHOTS.files_are_parquet

    def test_get_profile_activitylog(self):
        from common.dataset_profile import get_profile, ACTIVITYLOG
        assert get_profile("activitylog") is ACTIVITYLOG

    def test_get_profile_snapshots(self):
        from common.dataset_profile import get_profile, SNAPSHOTS
        assert get_profile("snapshots") is SNAPSHOTS

    def test_get_profile_case_insensitive(self):
        from common.dataset_profile import get_profile, ACTIVITYLOG
        assert get_profile("ActivityLog") is ACTIVITYLOG

    def test_get_profile_invalid(self):
        from common.dataset_profile import get_profile
        with pytest.raises(ValueError, match="Unknown dataset profile"):
            get_profile("unknown")


# ---------------------------------------------------------------------------
# extract_segment_value_from_path
# ---------------------------------------------------------------------------

class TestExtractSegmentValue:
    def test_extract_eventtype(self):
        from storage.base import extract_segment_value_from_path
        path = "pod=a/siteLuid=b/y=2026/m=06/d=08/h=14/eventType=Login/file.txt"
        assert extract_segment_value_from_path(path, "eventType") == "Login"

    def test_extract_entitytype(self):
        from storage.base import extract_segment_value_from_path
        path = "entityType=Users/y=2026/m=06/d=08/h=14/part-0.parquet"
        assert extract_segment_value_from_path(path, "entityType") == "Users"

    def test_extract_missing_key(self):
        from storage.base import extract_segment_value_from_path
        path = "y=2026/m=06/d=08/file.txt"
        assert extract_segment_value_from_path(path, "entityType") is None

    def test_extract_event_type_from_path_backcompat(self):
        from storage.base import extract_event_type_from_path
        path = "y=2026/m=06/d=08/h=14/eventType=Login/file.txt"
        assert extract_event_type_from_path(path) == "Login"

    def test_extract_empty_path(self):
        from storage.base import extract_segment_value_from_path
        assert extract_segment_value_from_path("", "entityType") is None

    def test_extract_at_end_of_path(self):
        from storage.base import extract_segment_value_from_path
        path = "something/entityType=Workbooks"
        assert extract_segment_value_from_path(path, "entityType") == "Workbooks"


# ---------------------------------------------------------------------------
# path_layout with segment_key="entityType"
# ---------------------------------------------------------------------------

class TestPathLayoutEntityType:
    def test_map_api_to_local_date_first(self):
        from extractor.path_layout import map_api_to_local_relpath, ExtractPathLayout
        path = "y=2026/m=06/d=08/h=14/entityType=Users/part-0.parquet"
        result = map_api_to_local_relpath(path, ExtractPathLayout.DATE_FIRST, "entityType")
        assert result == path

    def test_map_api_to_local_schema_first(self):
        from extractor.path_layout import map_api_to_local_relpath, ExtractPathLayout
        path = "y=2026/m=06/d=08/h=14/entityType=Users/part-0.parquet"
        result = map_api_to_local_relpath(path, ExtractPathLayout.SCHEMA_FIRST, "entityType")
        assert result == "entityType=Users/y=2026/m=06/d=08/h=14/part-0.parquet"

    def test_schema_first_round_trip(self):
        from extractor.path_layout import map_api_to_local_relpath, ExtractPathLayout
        api_path = "pod=a/siteLuid=b/y=2026/m=06/d=08/h=14/entityType=Groups/f.parquet"
        schema_first = map_api_to_local_relpath(api_path, ExtractPathLayout.SCHEMA_FIRST, "entityType")
        assert "entityType=Groups" in schema_first
        # entityType= should appear before y= in schema-first
        assert schema_first.index("entityType=Groups") < schema_first.index("y=2026")

    def test_classify_entity_date_first(self):
        from extractor.path_layout import _classify_path_if_known, ExtractPathLayout
        path = "y=2026/m=06/d=08/h=14/entityType=Users/f.parquet"
        assert _classify_path_if_known(path, "entityType") == ExtractPathLayout.DATE_FIRST

    def test_classify_entity_schema_first(self):
        from extractor.path_layout import _classify_path_if_known, ExtractPathLayout
        path = "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet"
        assert _classify_path_if_known(path, "entityType") == ExtractPathLayout.SCHEMA_FIRST


# ---------------------------------------------------------------------------
# storage/presence.py with partition_key="entityType"
# ---------------------------------------------------------------------------

class TestPresenceComputePrefixes:
    def test_schema_first_entitytype(self):
        from storage.presence import _compute_list_prefixes
        paths = ["entityType=Users/y=2026/m=06/d=08/h=14/f.parquet"]
        prefixes = _compute_list_prefixes(paths, "schema_first", "entityType")
        assert any("entityType=Users" in p for p in prefixes)
        assert any("y=2026" in p for p in prefixes)

    def test_date_first_entitytype(self):
        from storage.presence import _compute_list_prefixes
        paths = ["y=2026/m=06/d=08/h=14/entityType=Users/f.parquet"]
        prefixes = _compute_list_prefixes(paths, "date_first", "entityType")
        # date-first doesn't include entityType in prefix
        assert any("y=2026" in p for p in prefixes)
        assert all("entityType" not in p for p in prefixes)

    def test_multi_entity_type_schema_first(self):
        from storage.presence import _compute_list_prefixes
        paths = [
            "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet",
            "entityType=Groups/y=2026/m=06/d=08/h=14/f.parquet",
        ]
        prefixes = _compute_list_prefixes(paths, "schema_first", "entityType")
        entity_prefixes = [p for p in prefixes if "entityType=Users" in p or "entityType=Groups" in p]
        assert len(entity_prefixes) == 2


# ---------------------------------------------------------------------------
# LogRetriever._select_latest_per_partition
# ---------------------------------------------------------------------------

class TestSelectLatestPerPartition:
    def _make_retriever(self):
        from common.dataset_profile import SNAPSHOTS
        from extractor.retriever import LogRetriever
        from storage.local import LocalStorage
        import tempfile, os
        tmp = tempfile.mkdtemp()
        storage = LocalStorage(tmp)
        return LogRetriever(
            pat_name="x", pat_secret="x", api_url="http://x",
            site_ids=[], start_time="", end_time="",
            storage_backend=storage,
            dataset_profile=SNAPSHOTS,
        )

    def test_keeps_newest_per_entity_type(self):
        r = self._make_retriever()
        files = [
            {"path": "entityType=Users/y=2026/m=06/d=07/h=14/f.parquet"},
            {"path": "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet"},  # newer
            {"path": "entityType=Groups/y=2026/m=06/d=08/h=10/f.parquet"},
        ]
        result = r._select_latest_per_partition(files)
        paths = [e["path"] for e in result]
        assert "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet" in paths
        assert "entityType=Users/y=2026/m=06/d=07/h=14/f.parquet" not in paths
        assert "entityType=Groups/y=2026/m=06/d=08/h=10/f.parquet" in paths

    def test_preserves_undated_files(self):
        r = self._make_retriever()
        files = [
            {"path": "some/undated/file.parquet"},
            {"path": "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet"},
        ]
        result = r._select_latest_per_partition(files)
        paths = [e["path"] for e in result]
        assert "some/undated/file.parquet" in paths

    def test_empty_list(self):
        r = self._make_retriever()
        assert r._select_latest_per_partition([]) == []

    def test_site_id_tag_preserved_through_selection(self):
        """_site_id tags must survive _select_latest_per_partition so presigned URLs
        can be routed back to the correct site-scoped endpoint."""
        r = self._make_retriever()
        files = [
            {"path": "entityType=Users/y=2026/m=06/d=07/h=14/f.parquet", "_site_id": "site-a"},
            {"path": "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet", "_site_id": "site-a"},  # newer
            {"path": "entityType=Groups/y=2026/m=06/d=08/h=10/f.parquet", "_site_id": "site-b"},
        ]
        result = r._select_latest_per_partition(files)
        by_path = {e["path"]: e for e in result}
        assert by_path["entityType=Users/y=2026/m=06/d=08/h=14/f.parquet"]["_site_id"] == "site-a"
        assert by_path["entityType=Groups/y=2026/m=06/d=08/h=10/f.parquet"]["_site_id"] == "site-b"

    def test_each_site_retains_its_own_newest_when_hours_differ(self):
        """When two sites have the same entity type but their newest snapshot
        arrived in different hours, both sites must retain their own newest file.
        Previously, global grouping by entityType alone would drop the lagging
        site entirely."""
        r = self._make_retriever()
        files = [
            # site-a: newest Users snapshot is h=21
            {"path": "entityType=Users/y=2026/m=06/d=08/h=20/f.parquet", "_site_id": "site-a"},
            {"path": "entityType=Users/y=2026/m=06/d=08/h=21/f.parquet", "_site_id": "site-a"},
            # site-b: newest Users snapshot is h=19 (one hour behind)
            {"path": "entityType=Users/y=2026/m=06/d=08/h=18/f.parquet", "_site_id": "site-b"},
            {"path": "entityType=Users/y=2026/m=06/d=08/h=19/f.parquet", "_site_id": "site-b"},
        ]
        result = r._select_latest_per_partition(files)
        paths = {e["path"] for e in result}
        # Each site keeps its own newest; older hours are dropped
        assert "entityType=Users/y=2026/m=06/d=08/h=21/f.parquet" in paths  # site-a newest
        assert "entityType=Users/y=2026/m=06/d=08/h=20/f.parquet" not in paths  # site-a older
        assert "entityType=Users/y=2026/m=06/d=08/h=19/f.parquet" in paths  # site-b newest
        assert "entityType=Users/y=2026/m=06/d=08/h=18/f.parquet" not in paths  # site-b older
        # Exactly two files should be selected (one per site)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# LogRetriever._cadence_default_window
# ---------------------------------------------------------------------------

class TestCadenceDefaultWindow:
    def _make_retriever(self, cadence="hourly", intervals=4):
        from common.dataset_profile import SNAPSHOTS
        from extractor.retriever import LogRetriever
        from storage.local import LocalStorage
        import tempfile
        tmp = tempfile.mkdtemp()
        storage = LocalStorage(tmp)
        return LogRetriever(
            pat_name="x", pat_secret="x", api_url="http://x",
            site_ids=[], start_time="", end_time="",
            storage_backend=storage,
            dataset_profile=SNAPSHOTS,
            snapshot_cadence=cadence,
            snapshot_lookback_intervals=intervals,
        )

    def test_hourly_window_length(self):
        from extractor.retriever import _parse_iso_datetime
        r = self._make_retriever("hourly", 4)
        start, end = r._cadence_default_window()
        start_dt = _parse_iso_datetime(start)
        end_dt = _parse_iso_datetime(end)
        diff_hours = (end_dt - start_dt).total_seconds() / 3600
        assert 3.9 < diff_hours < 4.1

    def test_daily_window_length(self):
        from extractor.retriever import _parse_iso_datetime
        r = self._make_retriever("daily", 2)
        start, end = r._cadence_default_window()
        start_dt = _parse_iso_datetime(start)
        end_dt = _parse_iso_datetime(end)
        diff_days = (end_dt - start_dt).total_seconds() / 86400
        assert 1.9 < diff_days < 2.1


# ---------------------------------------------------------------------------
# LogRetriever._check_delivery_gaps
# ---------------------------------------------------------------------------

class TestDeliveryGapDetection:
    def _make_retriever(self, gap_action="warn"):
        from common.dataset_profile import SNAPSHOTS
        from extractor.retriever import LogRetriever
        from storage.local import LocalStorage
        import tempfile
        tmp = tempfile.mkdtemp()
        storage = LocalStorage(tmp)
        return LogRetriever(
            pat_name="x", pat_secret="x", api_url="http://x",
            site_ids=[], start_time="", end_time="",
            storage_backend=storage,
            dataset_profile=SNAPSHOTS,
            snapshot_cadence="hourly",
            snapshot_lookback_intervals=4,
            snapshot_gap_action=gap_action,
        )

    def test_missing_requested_entity_warns(self, caplog):
        import logging
        r = self._make_retriever("warn")
        files = [{"path": "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet"}]
        with caplog.at_level(logging.WARNING, logger="extractor.retriever"):
            r._check_delivery_gaps(files, ["Users", "Groups"])
        assert any("Groups" in msg for msg in caplog.messages)

    def test_no_gap_no_warning(self, caplog):
        import logging, datetime
        r = self._make_retriever("warn")
        now = datetime.datetime.now(datetime.timezone.utc)
        # Fresh file with very recent date
        files = [{
            "path": f"entityType=Users/y={now.year}/m={now.month:02d}/d={now.day:02d}/h={now.hour:02d}/f.parquet"
        }]
        with caplog.at_level(logging.WARNING, logger="extractor.retriever"):
            r._check_delivery_gaps(files, ["Users"])
        gap_warnings = [m for m in caplog.messages if "gap" in m.lower() or "stale" in m.lower() or "missing" in m.lower()]
        assert not gap_warnings

    def test_error_action_raises(self):
        r = self._make_retriever("error")
        files = []  # no files
        with pytest.raises(RuntimeError, match="delivery gap"):
            r._check_delivery_gaps(files, ["Users"])

    def test_ignore_action_no_exception(self):
        r = self._make_retriever("ignore")
        files = []
        r._check_delivery_gaps(files, ["Users", "Groups"])  # should not raise

    def test_stale_site_flagged_even_when_other_site_is_fresh(self, caplog):
        """A site whose newest snapshot is stale must appear in the gap warning even
        when another site has a fresh snapshot for the same entity type.  Previously,
        global grouping masked the lagging site."""
        import logging, datetime
        r = self._make_retriever("warn")
        now = datetime.datetime.now(datetime.timezone.utc)
        # site-a: Users snapshot is stale (48+ hours old for an hourly cadence with 2h grace)
        stale_dt = now - datetime.timedelta(hours=6)
        # site-b: Users snapshot is fresh (current hour)
        fresh_dt = now
        files = [
            {
                "path": (
                    f"entityType=Users/y={stale_dt.year}/m={stale_dt.month:02d}"
                    f"/d={stale_dt.day:02d}/h={stale_dt.hour:02d}/f.parquet"
                ),
                "_site_id": "site-a",
            },
            {
                "path": (
                    f"entityType=Users/y={fresh_dt.year}/m={fresh_dt.month:02d}"
                    f"/d={fresh_dt.day:02d}/h={fresh_dt.hour:02d}/f.parquet"
                ),
                "_site_id": "site-b",
            },
        ]
        with caplog.at_level(logging.WARNING, logger="extractor.retriever"):
            r._check_delivery_gaps(files, None)
        stale_warnings = [m for m in caplog.messages if "site-a" in m]
        assert stale_warnings, "Expected a gap warning mentioning site-a"
        # site-b is fresh so it should not appear in a stale warning
        assert not any("site-b" in m for m in caplog.messages if "gap" in m.lower() or "stale" in m.lower())

    def test_missing_entity_message_mentions_any_site(self, caplog):
        """When a requested entity type is absent, the warning should clarify
        that it was missing across all sites."""
        import logging
        r = self._make_retriever("warn")
        files = [{"path": "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet", "_site_id": "site-a"}]
        with caplog.at_level(logging.WARNING, logger="extractor.retriever"):
            r._check_delivery_gaps(files, ["Users", "Groups"])
        assert any("Groups" in m and "any site" in m for m in caplog.messages)


# ---------------------------------------------------------------------------
# LogRetriever URL construction with SNAPSHOTS profile
# ---------------------------------------------------------------------------

class TestRetrieverUrlConstruction:
    """Verify that the SNAPSHOTS profile produces /snapshots/ URLs instead of /activitylog/."""

    def _make_retriever(self, profile):
        from extractor.retriever import LogRetriever
        from storage.local import LocalStorage
        import tempfile
        tmp = tempfile.mkdtemp()
        storage = LocalStorage(tmp)
        r = LogRetriever(
            pat_name="x", pat_secret="x", api_url="https://tcm.example.com",
            site_ids=["site-uuid-1"], start_time="2026-06-08T00:00:00Z", end_time="2026-06-08T04:00:00Z",
            storage_backend=storage,
            dataset_profile=profile,
        )
        r.session_token = "test-token"
        r.tenant_id = "tenant-uuid"
        return r

    def test_activitylog_list_url(self):
        from common.dataset_profile import ACTIVITYLOG
        from unittest.mock import patch
        r = self._make_retriever(ACTIVITYLOG)
        with patch.object(r, "_list_files", return_value=[]) as mock_list:
            r._get_file_list_for_event_types("op", r.start_time, r.end_time, None, site_id="site-uuid-1")
        args, kwargs = mock_list.call_args
        # The URL is constructed inside _list_files; verify it calls with expected args
        assert "site_id" in mock_list.call_args.kwargs or (len(args) > 4)

    def test_snapshots_filter_param(self):
        from common.dataset_profile import SNAPSHOTS
        r = self._make_retriever(SNAPSHOTS)
        assert r.profile.filter_param == "entityType"
        assert r.profile.resource == "snapshots"


# ---------------------------------------------------------------------------
# transformer reader.select_latest_per_partition
# ---------------------------------------------------------------------------

class TestReaderSelectLatest:
    def test_keeps_newest(self):
        from transformer.reader import select_latest_per_partition
        files = [
            "entityType=Users/y=2026/m=06/d=07/h=14/f.parquet",
            "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet",  # newest
            "entityType=Groups/y=2026/m=06/d=08/h=10/f.parquet",
        ]
        result = select_latest_per_partition(files, "entityType")
        assert "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet" in result
        assert "entityType=Users/y=2026/m=06/d=07/h=14/f.parquet" not in result
        assert len(result) == 2

    def test_all_files_when_one_per_type(self):
        from transformer.reader import select_latest_per_partition
        files = [
            "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet",
            "entityType=Groups/y=2026/m=06/d=08/h=14/f.parquet",
        ]
        result = select_latest_per_partition(files, "entityType")
        assert len(result) == 2

    def test_preserves_ungrouped(self):
        from transformer.reader import select_latest_per_partition
        files = ["no/partition/here.parquet"]
        result = select_latest_per_partition(files, "entityType")
        assert result == files


class TestSelectFilesForMapping:
    def test_all_mode_unchanged(self):
        from transformer.reader import select_files_for_mapping
        files = ["a.parquet", "b.parquet"]
        config = {"select": "all"}
        assert select_files_for_mapping(files, config) == files

    def test_latest_per_partition(self):
        from transformer.reader import select_files_for_mapping
        files = [
            "entityType=Users/y=2026/m=06/d=07/h=14/f.parquet",
            "entityType=Users/y=2026/m=06/d=08/h=14/f.parquet",
        ]
        config = {"select": "latest_per_partition", "partition_field": "entityType"}
        result = select_files_for_mapping(files, config)
        assert len(result) == 1
        assert "d=08" in result[0]

    def test_all_snapshots_stub_falls_back_to_all(self):
        from transformer.reader import select_files_for_mapping
        files = ["a.parquet", "b.parquet"]
        config = {"select": "all_snapshots"}
        # Should warn and return all files
        result = select_files_for_mapping(files, config)
        assert result == files

    def test_unknown_mode_falls_back(self):
        from transformer.reader import select_files_for_mapping
        files = ["a.parquet"]
        config = {"select": "bogus_mode"}
        result = select_files_for_mapping(files, config)
        assert result == files


# ---------------------------------------------------------------------------
# load_state snapshot watermark
# ---------------------------------------------------------------------------

class TestSnapshotWatermark:
    def test_no_state_returns_none(self, tmp_path):
        from loader.load_state import get_last_snapshot_time
        hyper_path = str(tmp_path / "bundle.hyper")
        assert get_last_snapshot_time(hyper_path, "Users") is None

    def test_set_and_get(self, tmp_path):
        from loader.load_state import get_last_snapshot_time, set_last_snapshot_time
        hyper_path = str(tmp_path / "bundle.hyper")
        set_last_snapshot_time(hyper_path, "Users", "2026-06-08T14:00:00Z")
        assert get_last_snapshot_time(hyper_path, "Users") == "2026-06-08T14:00:00Z"

    def test_different_tables_independent(self, tmp_path):
        from loader.load_state import get_last_snapshot_time, set_last_snapshot_time
        hyper_path = str(tmp_path / "bundle.hyper")
        set_last_snapshot_time(hyper_path, "Users", "2026-06-08T14:00:00Z")
        set_last_snapshot_time(hyper_path, "Groups", "2026-06-08T10:00:00Z")
        assert get_last_snapshot_time(hyper_path, "Users") == "2026-06-08T14:00:00Z"
        assert get_last_snapshot_time(hyper_path, "Groups") == "2026-06-08T10:00:00Z"

    def test_coexists_with_increment_max(self, tmp_path):
        from loader.load_state import (
            get_last_snapshot_time, set_last_snapshot_time,
            get_last_increment_max, set_last_increment_max,
        )
        hyper_path = str(tmp_path / "bundle.hyper")
        set_last_increment_max(hyper_path, "ActivityLog", "2026-06-08T12:00:00Z")
        set_last_snapshot_time(hyper_path, "Users", "2026-06-08T14:00:00Z")
        # Both coexist in the same state file.
        assert get_last_increment_max(hyper_path, "ActivityLog") == "2026-06-08T12:00:00Z"
        assert get_last_snapshot_time(hyper_path, "Users") == "2026-06-08T14:00:00Z"


# ---------------------------------------------------------------------------
# loader_mapping snapshot validation
# ---------------------------------------------------------------------------

class TestLoaderMappingSnapshotValidation:
    def _make_config(self, tables):
        return {
            "target": {"type": "hyper", "hyper_path": "/tmp/test.hyper"},
            "tables": tables,
        }

    def test_snapshot_table_valid(self):
        from loader.loader_mapping import validate_loader_mapping
        config = self._make_config([{
            "table": "Users",
            "write_mode": "snapshot",
            "snapshot_source_dir": "/tmp/snapshots",
        }])
        errors = validate_loader_mapping(config)
        assert not errors

    def test_strict_snapshot_table_valid(self):
        from loader.loader_mapping import validate_loader_mapping
        config = self._make_config([{
            "table": "Users",
            "write_mode": "strict_snapshot",
            "snapshot_source_dir": "/tmp/snapshots",
        }])
        errors = validate_loader_mapping(config)
        assert not errors

    def test_snapshot_missing_source_dir(self):
        from loader.loader_mapping import validate_loader_mapping
        config = self._make_config([{
            "table": "Users",
            "write_mode": "snapshot",
        }])
        errors = validate_loader_mapping(config)
        assert any("snapshot_source_dir" in e for e in errors)

    def test_snapshot_with_transformer_mappings_invalid(self):
        from loader.loader_mapping import validate_loader_mapping
        config = self._make_config([{
            "table": "Users",
            "write_mode": "snapshot",
            "snapshot_source_dir": "/tmp/snapshots",
            "transformer_mappings": ["some.yaml"],
        }])
        errors = validate_loader_mapping(config)
        assert any("snapshot" in e.lower() for e in errors)

    def test_get_snapshot_table_configs(self):
        from loader.loader_mapping import get_snapshot_table_configs
        config = self._make_config([
            {
                "table": "Users",
                "write_mode": "snapshot",
                "snapshot_source_dir": "{storage_path}/transform/snapshots",
                "schema": "Snapshots",
            },
            {
                "table": "ActivityLog",
                "write_mode": "append",
                "transformer_mappings": ["log.yaml"],
                "increment_field": "eventTime",
            },
        ])
        snap_cfgs = get_snapshot_table_configs(config, "/data")
        assert len(snap_cfgs) == 1
        assert snap_cfgs[0]["table"] == "Users"
        assert snap_cfgs[0]["snapshot_source_dir"] == "/data/transform/snapshots"
        assert snap_cfgs[0]["schema"] == "Snapshots"

    def test_get_snapshot_table_configs_includes_drift_policy(self):
        from loader.loader_mapping import get_snapshot_table_configs
        config = self._make_config([
            {
                "table": "Users",
                "write_mode": "snapshot",
                "snapshot_source_dir": "/tmp/snapshots",
            },
            {
                "table": "Groups",
                "write_mode": "strict_snapshot",
                "snapshot_source_dir": "/tmp/snapshots",
            },
        ])
        snap_cfgs = get_snapshot_table_configs(config, "/data")
        by_table = {c["table"]: c for c in snap_cfgs}
        assert by_table["Users"]["drift_policy"] == "allow"
        assert by_table["Groups"]["drift_policy"] == "fail"


# ---------------------------------------------------------------------------
# loader._read_snapshot_manifest
# ---------------------------------------------------------------------------

class TestReadSnapshotManifest:
    def test_reads_manifest(self, tmp_path):
        from loader.loader import _read_snapshot_manifest
        manifest = {
            "Users": {"snapshot_time": "2026-06-08T14:00:00Z", "filename": "Users.parquet"},
        }
        manifest_path = tmp_path / "_manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = _read_snapshot_manifest(str(tmp_path))
        assert result["Users"]["snapshot_time"] == "2026-06-08T14:00:00Z"

    def test_missing_manifest_returns_empty(self, tmp_path):
        from loader.loader import _read_snapshot_manifest
        result = _read_snapshot_manifest(str(tmp_path))
        assert result == {}


# ---------------------------------------------------------------------------
# load_snapshot_tables — drop-replace semantics
# ---------------------------------------------------------------------------

class TestLoadSnapshotTablesDropReplace:
    """
    Tests for the drop-before-replace mechanic in load_snapshot_tables.

    write_parquet_to_hyper, get_hyper_table_columns, drop_hyper_table, and
    hyper_file_exists are all mocked so no actual Hyper file is needed.
    Real PyArrow Parquet files are written so pq.read_table (used by the
    strict_snapshot drift check) works correctly.
    """

    # ------------------------------------------------------------------ helpers

    def _write_parquet(self, path, schema_a=False):
        """
        Write a tiny Parquet file.
        schema_a=False  →  columns: id (int64), name (str)
        schema_a=True   →  same + siteLuid (str)  [the added column]
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        cols = {
            "id":   pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["alice", "bob"]),
        }
        if schema_a:
            cols["siteLuid"] = pa.array(["siteA", "siteA"])
        pq.write_table(pa.table(cols), path)

    def _make_snap_dir(self, tmp_path, table="Users", snapshot_time="2026-06-08T14:00:00Z",
                       with_siteluid=False):
        """Create a snapshot dir with a Parquet file and _manifest.json."""
        import json
        snap_dir = str(tmp_path / "snapshots")
        os.makedirs(snap_dir, exist_ok=True)
        parquet_path = os.path.join(snap_dir, f"{table}.parquet")
        self._write_parquet(parquet_path, schema_a=with_siteluid)
        manifest = {
            table: {"snapshot_time": snapshot_time, "filename": f"{table}.parquet"},
        }
        (tmp_path / "snapshots" / "_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        return snap_dir

    def _loader_config(self, snap_dir, table="Users", write_mode="snapshot"):
        return {
            "target": {"type": "hyper", "hyper_path": "/irrelevant/bundle.hyper"},
            "tables": [{
                "table": table,
                "schema": "snapshots",
                "write_mode": write_mode,
                "snapshot_source_dir": snap_dir,
            }],
        }

    # ------------------------------------------------------------------ tests

    def test_snapshot_allow_schema_change_absorbed(self, tmp_path):
        """snapshot (allow): adding siteLuid succeeds; table has the new column count."""
        from unittest.mock import patch, MagicMock

        snap_dir = self._make_snap_dir(tmp_path, with_siteluid=True)
        hyper_path = str(tmp_path / "bundle.hyper")
        config = self._loader_config(snap_dir)

        # Pre-existing table has only two columns (old schema without siteLuid).
        old_cols = [("id", None), ("name", None)]

        with patch("loader.loader.hyper_file_exists", return_value=True), \
             patch("loader.loader.get_hyper_table_columns", return_value=old_cols), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper", return_value=2) as mock_write, \
             patch("loader.loader.get_last_snapshot_time", return_value="2026-06-07T00:00:00Z"), \
             patch("loader.loader.set_last_snapshot_time") as mock_set_wm:

            rows, _ = __import__("loader.loader", fromlist=["load_snapshot_tables"]).load_snapshot_tables(
                config, "/data", hyper_path, False, [], [False], "bulk"
            )

        assert rows == 2
        mock_drop.assert_called_once_with(hyper_path, "snapshots", "Users")
        mock_write.assert_called_once()
        mock_set_wm.assert_called_once_with(hyper_path, "Users", "2026-06-08T14:00:00Z")

    def test_snapshot_no_duplication_on_unchanged_schema(self, tmp_path):
        """snapshot (allow): same columns, newer snapshot → drop+replace, not append."""
        from unittest.mock import patch

        snap_dir = self._make_snap_dir(tmp_path)
        hyper_path = str(tmp_path / "bundle.hyper")
        config = self._loader_config(snap_dir)

        existing_cols = [("id", None), ("name", None)]

        with patch("loader.loader.hyper_file_exists", return_value=True), \
             patch("loader.loader.get_hyper_table_columns", return_value=existing_cols), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper", return_value=2), \
             patch("loader.loader.get_last_snapshot_time", return_value="2026-06-07T00:00:00Z"), \
             patch("loader.loader.set_last_snapshot_time"):

            __import__("loader.loader", fromlist=["load_snapshot_tables"]).load_snapshot_tables(
                config, "/data", hyper_path, False, [], [False], "bulk"
            )

        # Must have dropped the old table before writing.
        mock_drop.assert_called_once_with(hyper_path, "snapshots", "Users")

    def test_watermark_gating_skips_old_snapshot(self, tmp_path):
        """A snapshot whose time is ≤ the stored watermark is skipped entirely."""
        from unittest.mock import patch

        snap_dir = self._make_snap_dir(tmp_path, snapshot_time="2026-06-06T00:00:00Z")
        hyper_path = str(tmp_path / "bundle.hyper")
        config = self._loader_config(snap_dir)

        with patch("loader.loader.hyper_file_exists", return_value=True), \
             patch("loader.loader.get_hyper_table_columns", return_value=[("id", None)]), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper") as mock_write, \
             patch("loader.loader.get_last_snapshot_time", return_value="2026-06-08T00:00:00Z"), \
             patch("loader.loader.set_last_snapshot_time") as mock_set_wm:

            rows, _ = __import__("loader.loader", fromlist=["load_snapshot_tables"]).load_snapshot_tables(
                config, "/data", hyper_path, False, [], [False], "bulk"
            )

        assert rows == 0
        mock_drop.assert_not_called()
        mock_write.assert_not_called()
        mock_set_wm.assert_not_called()

    def test_strict_snapshot_halts_on_drift(self, tmp_path):
        """strict_snapshot (fail): added siteLuid column raises RuntimeError; watermark not advanced."""
        import pytest
        from unittest.mock import patch

        snap_dir = self._make_snap_dir(tmp_path, with_siteluid=True)
        hyper_path = str(tmp_path / "bundle.hyper")
        config = self._loader_config(snap_dir, write_mode="strict_snapshot")

        # Existing table has only two columns — drift when siteLuid arrives.
        old_cols = [("id", None), ("name", None)]

        with patch("loader.loader.hyper_file_exists", return_value=True), \
             patch("loader.loader.get_hyper_table_columns", return_value=old_cols), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper") as mock_write, \
             patch("loader.loader.get_last_snapshot_time", return_value="2026-06-07T00:00:00Z"), \
             patch("loader.loader.set_last_snapshot_time") as mock_set_wm:

            with pytest.raises(RuntimeError):
                __import__("loader.loader", fromlist=["load_snapshot_tables"]).load_snapshot_tables(
                    config, "/data", hyper_path, False, [], [False], "bulk"
                )

        # Neither a drop nor a write nor a watermark advance should have happened.
        mock_drop.assert_not_called()
        mock_write.assert_not_called()
        mock_set_wm.assert_not_called()

    def test_first_run_no_drop(self, tmp_path):
        """On the very first run (no watermark, no existing table) no drop is issued."""
        from unittest.mock import patch

        snap_dir = self._make_snap_dir(tmp_path)
        hyper_path = str(tmp_path / "bundle.hyper")
        config = self._loader_config(snap_dir)

        with patch("loader.loader.hyper_file_exists", return_value=False), \
             patch("loader.loader.get_hyper_table_columns", return_value=None), \
             patch("loader.loader.drop_hyper_table") as mock_drop, \
             patch("loader.loader.write_parquet_to_hyper", return_value=2), \
             patch("loader.loader.get_last_snapshot_time", return_value=None), \
             patch("loader.loader.set_last_snapshot_time"):

            rows, _ = __import__("loader.loader", fromlist=["load_snapshot_tables"]).load_snapshot_tables(
                config, "/data", hyper_path, True, [], [False], "bulk"
            )

        assert rows == 2
        mock_drop.assert_not_called()
