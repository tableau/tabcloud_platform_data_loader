"""Tests for runner.validate — static pre-flight validation."""

from __future__ import annotations

import os
import textwrap
import pytest

from runner.config import (
    RunConfig, ConnectionConfig, ExtractionJobConfig, TransformJobConfig,
    LoadJobConfig, DestinationConfig, StorageConfig, LoggingConfig,
    DATA_EVENT_LOGS, DATA_SNAPSHOTS, DATA_REFERENCE,
    bundled_mappings_root,
)
from runner.validate import validate, ValidationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _base_cfg(**overrides) -> RunConfig:
    kwargs = dict(
        version=1,
        base_dir="/tmp",
        connection=ConnectionConfig(
            tcm_url="https://example.online.tableau.com",
            token_name="pat",
            token_secret="keyring:tabcloud/pat",
        ),
        extraction={},
        transform={},
        load={},
        destinations={},
        storage=StorageConfig(path="/tmp/data"),
        logging_cfg=LoggingConfig(),
    )
    kwargs.update(overrides)
    return RunConfig(**kwargs)


def _dest(name="prod", **kw) -> DestinationConfig:
    defaults = dict(
        name=name,
        type="tableau",
        server_url="https://pod.online.tableau.com",
        token_name="pub-pat",
        token_secret="keyring:tabcloud/publish-pat",
        site="",
        project="",
        mode="upsert",
    )
    defaults.update(kw)
    return DestinationConfig(**defaults)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


class TestConnectionValidation:
    def test_valid_connection_no_errors(self):
        cfg = _base_cfg()
        r = validate(cfg)
        assert r.ok

    def test_missing_tcm_url(self):
        cfg = _base_cfg()
        cfg.connection.tcm_url = ""
        r = validate(cfg)
        assert not r.ok
        assert any("tcm_url" in e for e in r.errors)

    def test_bad_tcm_url_with_path(self):
        cfg = _base_cfg()
        cfg.connection.tcm_url = "https://example.com/api/v1"
        r = validate(cfg)
        assert any("tcm_url" in e for e in r.errors)

    def test_http_not_https(self):
        cfg = _base_cfg()
        cfg.connection.tcm_url = "http://example.com"
        r = validate(cfg)
        assert any("tcm_url" in e for e in r.errors)

    def test_plaintext_secret_emits_warning_not_error(self):
        cfg = _base_cfg()
        cfg.connection.token_secret = "plain-secret"
        r = validate(cfg)
        # Should be a warning, not an error (we don't block on this)
        assert r.ok  # still ok
        assert any("plain" in w.lower() or "secret" in w.lower() for w in r.warnings)

    def test_plaintext_secret_value_not_in_warning_text(self):
        """The raw secret must never appear in any warning or error message."""
        raw_secret = "AB/Sv8v+QdafITmoMK+TjA==:ItYSTAqCdyDO546X_bwRzklnPiDgJ94dqfB5bEPNaIg"
        cfg = _base_cfg()
        cfg.connection.token_secret = raw_secret
        r = validate(cfg)
        for msg in r.warnings + r.errors:
            assert raw_secret not in msg, f"Secret leaked into message: {msg!r}"


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


class TestTransformValidation:
    def test_valid_mapping_file_passes(self, tmp_path):
        mp = tmp_path / "events.yaml"
        mp.write_text("mapping_name: events\n")
        cfg = _base_cfg(transform={"events": TransformJobConfig(name="events", mapping=str(mp))})
        r = validate(cfg)
        assert r.ok

    def test_missing_mapping_file_errors(self):
        cfg = _base_cfg(transform={"events": TransformJobConfig(name="events", mapping="/nonexistent/path.yaml")})
        r = validate(cfg)
        assert not r.ok
        assert any("not found" in e for e in r.errors)

    def test_empty_mapping_path_errors(self):
        cfg = _base_cfg(transform={"events": TransformJobConfig(name="events", mapping="")})
        r = validate(cfg)
        assert not r.ok
        assert any("mapping" in e for e in r.errors)

    def test_defaulted_missing_mapping_explains_bundled_source(self):
        """When transform_defaulted=True and a mapping is absent, error explains where bundled files live."""
        cfg = _base_cfg(
            transform={"events": TransformJobConfig(name="events", mapping="/nonexistent/mappings/events.yaml")},
            transform_defaulted=True,
        )
        r = validate(cfg)
        assert not r.ok
        combined = "\n".join(r.errors)
        assert "bundled" in combined.lower() or "tabcloud-platform-data-loader" in combined
        assert "mappings/" in combined

    def test_non_defaulted_missing_mapping_has_plain_message(self):
        """When transform_defaulted=False the error is the simple 'file not found' message."""
        cfg = _base_cfg(
            transform={"events": TransformJobConfig(name="events", mapping="/nonexistent/path.yaml")},
            transform_defaulted=False,
        )
        r = validate(cfg)
        assert not r.ok
        assert any("file not found" in e for e in r.errors)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


class TestLoadValidation:
    def test_defaulted_missing_mapping_explains_bundled_source(self):
        """When load_defaulted=True and the mapping is absent, error explains where bundled file lives."""
        cfg = _base_cfg(
            load={"default": LoadJobConfig(name="default", mapping="/nonexistent/mappings/loader/bundle.yml")},
            load_defaulted=True,
        )
        r = validate(cfg)
        assert not r.ok
        combined = "\n".join(r.errors)
        assert "bundled" in combined.lower() or "tabcloud-platform-data-loader" in combined
        assert "mappings/" in combined

    def test_load_with_valid_destination(self, tmp_path):
        mp = tmp_path / "loader.yml"
        mp.write_text("load_name: test\n")
        cfg = _base_cfg(
            load={"l": LoadJobConfig(name="l", mapping=str(mp), destination="prod")},
            destinations={"prod": _dest()},
        )
        r = validate(cfg)
        # May have other warnings but should not have errors on destination resolution
        dest_errors = [e for e in r.errors if "destination" in e]
        assert not dest_errors

    def test_load_with_unresolved_destination_errors(self, tmp_path):
        mp = tmp_path / "loader.yml"
        mp.write_text("load_name: test\n")
        cfg = _base_cfg(
            load={"l": LoadJobConfig(name="l", mapping=str(mp), destination="missing")},
            destinations={},
        )
        r = validate(cfg)
        assert not r.ok
        assert any("missing" in e for e in r.errors)

    def test_load_no_destination_is_ok(self, tmp_path):
        mp = tmp_path / "loader.yml"
        mp.write_text("load_name: test\n")
        cfg = _base_cfg(
            load={"l": LoadJobConfig(name="l", mapping=str(mp), destination=None)},
        )
        r = validate(cfg)
        assert r.ok


# ---------------------------------------------------------------------------
# Destinations
# ---------------------------------------------------------------------------


class TestDestinationValidation:
    def test_valid_destination_no_errors(self):
        cfg = _base_cfg(destinations={"prod": _dest()})
        r = validate(cfg)
        assert r.ok

    def test_missing_server_url_errors(self):
        cfg = _base_cfg(destinations={"prod": _dest(server_url="")})
        r = validate(cfg)
        assert not r.ok
        assert any("server_url" in e for e in r.errors)

    def test_bad_server_url_errors(self):
        cfg = _base_cfg(destinations={"prod": _dest(server_url="https://pod.com/api/v1")})
        r = validate(cfg)
        assert not r.ok

    def test_invalid_mode_errors(self):
        cfg = _base_cfg(destinations={"prod": _dest(mode="bad_mode")})
        r = validate(cfg)
        assert not r.ok
        assert any("mode" in e for e in r.errors)

    def test_unsupported_type_errors(self):
        cfg = _base_cfg(destinations={"prod": _dest(type="snowflake")})
        r = validate(cfg)
        assert not r.ok
        assert any("snowflake" in e for e in r.errors)


# ---------------------------------------------------------------------------
# Output uniqueness
# ---------------------------------------------------------------------------


class TestOutputUniqueness:
    def test_duplicate_transform_mapping_warns(self, tmp_path):
        mp = tmp_path / "m.yaml"
        mp.write_text("x: 1\n")
        cfg = _base_cfg(transform={
            "a": TransformJobConfig(name="a", mapping=str(mp)),
            "b": TransformJobConfig(name="b", mapping=str(mp)),
        })
        r = validate(cfg)
        assert any("same mapping" in w for w in r.warnings)

    def test_duplicate_destination_datasource_errors(self, tmp_path):
        mp = tmp_path / "loader.yml"
        mp.write_text("load_name: test\n")
        job_a = LoadJobConfig(name="a", mapping=str(mp), destination="prod", datasource_name="DS")
        job_b = LoadJobConfig(name="b", mapping=str(mp), destination="prod", datasource_name="DS")
        cfg = _base_cfg(
            load={"a": job_a, "b": job_b},
            destinations={"prod": _dest()},
        )
        r = validate(cfg)
        assert not r.ok
        assert any("same data source name" in e for e in r.errors)

    def test_same_destination_different_datasource_ok(self, tmp_path):
        mp = tmp_path / "loader.yml"
        mp.write_text("load_name: test\n")
        job_a = LoadJobConfig(name="a", mapping=str(mp), destination="prod", datasource_name="DS-A")
        job_b = LoadJobConfig(name="b", mapping=str(mp), destination="prod", datasource_name="DS-B")
        cfg = _base_cfg(
            load={"a": job_a, "b": job_b},
            destinations={"prod": _dest()},
        )
        r = validate(cfg)
        dup_errors = [e for e in r.errors if "same data source name" in e]
        assert not dup_errors


# ---------------------------------------------------------------------------
# Extraction validation
# ---------------------------------------------------------------------------


class TestExtractionValidation:
    def test_invalid_data_type_errors(self):
        cfg = _base_cfg(extraction={"x": ExtractionJobConfig(name="x", data="bad_type")})
        r = validate(cfg)
        assert not r.ok
        assert any("bad_type" in e for e in r.errors)

    def test_invalid_scope_errors(self):
        cfg = _base_cfg(extraction={"x": ExtractionJobConfig(name="x", data=DATA_EVENT_LOGS, scope="cosmic")})
        r = validate(cfg)
        assert not r.ok
        assert any("scope" in e for e in r.errors)

    def test_start_without_end_errors(self):
        cfg = _base_cfg(extraction={
            "x": ExtractionJobConfig(name="x", data=DATA_EVENT_LOGS, start_time="2026-01-01T00:00:00Z")
        })
        r = validate(cfg)
        assert not r.ok
        assert any("start_time" in e and "end_time" in e for e in r.errors)

    def test_invalid_iso_timestamp_errors(self):
        cfg = _base_cfg(extraction={
            "x": ExtractionJobConfig(
                name="x", data=DATA_EVENT_LOGS,
                start_time="not-a-date", end_time="2026-01-01T00:00:00Z",
            )
        })
        r = validate(cfg)
        assert not r.ok
        assert any("ISO 8601" in e or "start_time" in e for e in r.errors)
