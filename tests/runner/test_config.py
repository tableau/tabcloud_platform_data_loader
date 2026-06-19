"""Tests for runner.config — RunConfig parsing, defaults, and path resolution."""

from __future__ import annotations

import os
import warnings
import pytest

from runner.config import (
    load_run_config,
    RunConfigError,
    DATA_EVENT_LOGS,
    DATA_SNAPSHOTS,
    DATA_REFERENCE,
    BACKEND_LOCAL,
    BACKEND_S3,
    resolve_window,
    sites_as_arg,
    ExtractionJobConfig,
    bundled_mappings_root,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(tmp_path, name, content):
    """Write content to a file, stripping leading newline from triple-quoted strings."""
    p = tmp_path / name
    # Strip a single leading newline so callers can use triple-quoted strings naturally.
    p.write_text(content.lstrip("\n"), encoding="utf-8")
    return str(p)


_MINIMAL = """
version: 1
connection:
  tcm_url:      https://example.online.tableau.com
  token_name:   my-pat
  token_secret: keyring:tabcloud/extractor-pat
"""


# ---------------------------------------------------------------------------
# Minimal / required fields
# ---------------------------------------------------------------------------


class TestMinimalConfig:
    def test_loads_minimal_yml(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL)
        cfg = load_run_config(path)
        assert cfg.connection.tcm_url == "https://example.online.tableau.com"
        assert cfg.connection.token_name == "my-pat"
        assert cfg.connection.token_secret == "keyring:tabcloud/extractor-pat"

    def test_version_defaults_to_1(self, tmp_path):
        content = """
connection:
  tcm_url:      https://example.online.tableau.com
  token_name:   my-pat
  token_secret: keyring:tabcloud/extractor-pat
"""
        path = _write(tmp_path, "run.yml", content)
        cfg = load_run_config(path)
        assert cfg.version == 1

    def test_missing_connection_raises(self, tmp_path):
        path = _write(tmp_path, "run.yml", "\nversion: 1\n")
        with pytest.raises(RunConfigError, match="connection"):
            load_run_config(path)

    def test_missing_tcm_url_raises(self, tmp_path):
        path = _write(tmp_path, "run.yml", """
connection:
  token_name: x
  token_secret: env:PAT
""")
        with pytest.raises(RunConfigError, match="tcm_url"):
            load_run_config(path)

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_run_config(str(tmp_path / "nonexistent.yml"))


# ---------------------------------------------------------------------------
# Default block synthesis
# ---------------------------------------------------------------------------


class TestDefaultBlocks:
    def test_absent_extraction_synthesizes_3_defaults(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL)
        cfg = load_run_config(path)
        assert cfg.extraction_defaulted is True
        assert set(cfg.extraction.keys()) == {"event_logs", "snapshots", "reference"}
        assert cfg.extraction["event_logs"].data == DATA_EVENT_LOGS
        assert cfg.extraction["snapshots"].data == DATA_SNAPSHOTS
        assert cfg.extraction["reference"].data == DATA_REFERENCE

    def test_absent_transform_synthesizes_3_defaults(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL)
        cfg = load_run_config(path)
        assert cfg.transform_defaulted is True
        assert set(cfg.transform.keys()) == {"events", "entity_snapshots", "tenant_reference"}

    def test_default_transform_mappings_resolve_under_bundled_root(self, tmp_path):
        """Default transform mappings must resolve under bundled_mappings_root(), not base_dir."""
        path = _write(tmp_path, "run.yml", _MINIMAL)
        cfg = load_run_config(path)
        root = bundled_mappings_root()
        for name, job in cfg.transform.items():
            assert job.mapping.startswith(root), (
                f"transform.{name}.mapping should start with bundled root {root!r}, "
                f"got {job.mapping!r}"
            )
            assert os.path.isfile(job.mapping), (
                f"Bundled default mapping for transform.{name} does not exist: {job.mapping!r}"
            )

    def test_absent_load_synthesizes_1_default(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL)
        cfg = load_run_config(path)
        assert cfg.load_defaulted is True
        assert "default" in cfg.load
        assert cfg.load["default"].destination is None

    def test_default_load_mapping_resolves_under_bundled_root(self, tmp_path):
        """Default load mapping must resolve under bundled_mappings_root(), not base_dir."""
        path = _write(tmp_path, "run.yml", _MINIMAL)
        cfg = load_run_config(path)
        root = bundled_mappings_root()
        job = cfg.load["default"]
        assert job.mapping.startswith(root), (
            f"load.default.mapping should start with bundled root {root!r}, got {job.mapping!r}"
        )
        assert os.path.isfile(job.mapping), (
            f"Bundled default loader mapping does not exist: {job.mapping!r}"
        )

    def test_explicit_extraction_not_defaulted(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
extraction:
  my_logs:
    data: event_logs
""")
        cfg = load_run_config(path)
        assert cfg.extraction_defaulted is False
        assert list(cfg.extraction.keys()) == ["my_logs"]

    def test_empty_extraction_block_warns_and_defaults(self, tmp_path):
        # YAML `extraction: {}` is an explicitly empty dict → triggers a warning.
        # YAML `extraction:` (null) is treated as absent (no warning, same as missing).
        path = _write(tmp_path, "run.yml", _MINIMAL + "\nextraction: {}\n")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cfg = load_run_config(path)
        assert cfg.extraction_defaulted is True
        assert any("extraction" in str(x.message).lower() for x in w)


# ---------------------------------------------------------------------------
# Extraction job parsing
# ---------------------------------------------------------------------------


class TestExtractionJobParsing:
    def test_event_logs_scope_tenant(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
extraction:
  tenant_logs:
    data: event_logs
    scope: tenant
""")
        cfg = load_run_config(path)
        assert cfg.extraction["tenant_logs"].scope == "tenant"

    def test_sites_as_list(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
extraction:
  logs:
    data: event_logs
    sites:
      - acme
      - contoso
""")
        cfg = load_run_config(path)
        job = cfg.extraction["logs"]
        assert job.sites == ["acme", "contoso"]

    def test_sites_all_string(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
extraction:
  logs:
    data: event_logs
    sites: all
""")
        cfg = load_run_config(path)
        assert cfg.extraction["logs"].sites == "all"

    def test_snapshots_cadence_daily(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
extraction:
  snaps:
    data: snapshots
    cadence: daily
    lookback_intervals: 7
""")
        cfg = load_run_config(path)
        job = cfg.extraction["snaps"]
        assert job.cadence == "daily"
        assert job.lookback_intervals == 7

    def test_reference_entities_list(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
extraction:
  ref:
    data: reference
    entities:
      - tenant_sites
""")
        cfg = load_run_config(path)
        assert cfg.extraction["ref"].entities == ["tenant_sites"]


# ---------------------------------------------------------------------------
# Storage / logging parsing
# ---------------------------------------------------------------------------


class TestStorageParsing:
    def test_local_storage_path_resolved(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
storage:
  backend: local
  path: ./workspace
""")
        cfg = load_run_config(path)
        assert cfg.storage.backend == BACKEND_LOCAL
        assert cfg.storage.path == os.path.abspath(os.path.join(str(tmp_path), "workspace"))

    def test_storage_defaults_to_local(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL)
        cfg = load_run_config(path)
        assert cfg.storage.backend == BACKEND_LOCAL

    def test_logging_level_parsed(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
logging:
  level: DEBUG
  retention_days: 30
""")
        cfg = load_run_config(path)
        assert cfg.logging_cfg.level == "DEBUG"
        assert cfg.logging_cfg.retention_days == 30


# ---------------------------------------------------------------------------
# Destinations
# ---------------------------------------------------------------------------


class TestDestinationsParsing:
    def test_destination_parsed(self, tmp_path):
        path = _write(tmp_path, "run.yml", _MINIMAL + """
destinations:
  prod:
    type: tableau
    server_url: https://pod.online.tableau.com
    token_name: pub-pat
    token_secret: keyring:tabcloud/publish-pat
    site: mysite
    project: Admin
    mode: upsert
""")
        cfg = load_run_config(path)
        dest = cfg.destinations["prod"]
        assert dest.server_url == "https://pod.online.tableau.com"
        assert dest.mode == "upsert"
        assert dest.site == "mysite"


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------


class TestWindowHelpers:
    def test_resolve_window_uses_explicit_times(self):
        job = ExtractionJobConfig(
            name="x", data=DATA_EVENT_LOGS,
            start_time="2026-01-01T00:00:00Z",
            end_time="2026-01-02T00:00:00Z",
        )
        start, end = resolve_window(job)
        assert start == "2026-01-01T00:00:00Z"
        assert end == "2026-01-02T00:00:00Z"

    def test_resolve_window_computes_lookback_when_absent(self):
        job = ExtractionJobConfig(name="x", data=DATA_EVENT_LOGS, lookback_days=2)
        start, end = resolve_window(job)
        assert start < end
        assert "T" in start and "T" in end

    def test_sites_as_arg_all(self):
        job = ExtractionJobConfig(name="x", data=DATA_EVENT_LOGS, sites="all")
        assert sites_as_arg(job) == "All"

    def test_sites_as_arg_list(self):
        job = ExtractionJobConfig(name="x", data=DATA_EVENT_LOGS, sites=["a", "b"])
        assert sites_as_arg(job) == "a,b"
