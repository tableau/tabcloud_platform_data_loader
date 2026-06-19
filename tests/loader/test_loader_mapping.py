"""
Unit tests for loader/loader_mapping.py: write_mode vocabulary, validation, and accessors.

Covers:
  - _WRITE_MODES: each mode maps to (strategy, drift_policy)
  - get_tables_with_write_modes: returns 7-tuples with strategy + drift_policy
  - validate_loader_mapping: write_mode validation, increment_field, snapshot_source_dir
  - resolve_write_mode: defaults and normalisation
  - get_snapshot_table_configs: drift_policy in returned dicts
"""

import sys
import os

import pytest

SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, os.path.abspath(SRC))

from loader.loader_mapping import (
    _WRITE_MODES,
    _INCREMENTAL_MODES,
    _SNAPSHOT_MODES,
    get_tables_with_write_modes,
    get_snapshot_table_configs,
    resolve_write_mode,
    validate_loader_mapping,
)


# ---------------------------------------------------------------------------
# _WRITE_MODES vocabulary
# ---------------------------------------------------------------------------

class TestWriteModesVocabulary:
    def test_all_six_modes_present(self):
        expected = {"replace", "strict_replace", "append", "strict_append", "snapshot", "strict_snapshot"}
        assert set(_WRITE_MODES.keys()) == expected

    def test_replace_strategy_allow(self):
        assert _WRITE_MODES["replace"] == ("replace", "allow")

    def test_strict_replace_strategy_fail(self):
        assert _WRITE_MODES["strict_replace"] == ("replace", "fail")

    def test_append_strategy_rebuild(self):
        assert _WRITE_MODES["append"] == ("incremental", "rebuild")

    def test_strict_append_strategy_fail(self):
        assert _WRITE_MODES["strict_append"] == ("incremental", "fail")

    def test_snapshot_strategy_allow(self):
        assert _WRITE_MODES["snapshot"] == ("snapshot", "allow")

    def test_strict_snapshot_strategy_fail(self):
        assert _WRITE_MODES["strict_snapshot"] == ("snapshot", "fail")

    def test_incremental_modes_set(self):
        assert _INCREMENTAL_MODES == frozenset({"append", "strict_append"})

    def test_snapshot_modes_set(self):
        assert _SNAPSHOT_MODES == frozenset({"snapshot", "strict_snapshot"})


# ---------------------------------------------------------------------------
# resolve_write_mode
# ---------------------------------------------------------------------------

class TestResolveWriteMode:
    def test_missing_write_mode_defaults_to_replace(self):
        mode, strategy, drift = resolve_write_mode({})
        assert mode == "replace"
        assert strategy == "replace"
        assert drift == "allow"

    def test_unknown_write_mode_defaults_to_replace(self):
        mode, strategy, drift = resolve_write_mode({"write_mode": "incremental"})
        assert mode == "replace"

    def test_append(self):
        mode, strategy, drift = resolve_write_mode({"write_mode": "append"})
        assert strategy == "incremental"
        assert drift == "rebuild"

    def test_strict_append(self):
        mode, strategy, drift = resolve_write_mode({"write_mode": "strict_append"})
        assert strategy == "incremental"
        assert drift == "fail"

    def test_snapshot(self):
        mode, strategy, drift = resolve_write_mode({"write_mode": "snapshot"})
        assert strategy == "snapshot"
        assert drift == "allow"

    def test_strict_snapshot(self):
        mode, strategy, drift = resolve_write_mode({"write_mode": "strict_snapshot"})
        assert strategy == "snapshot"
        assert drift == "fail"


# ---------------------------------------------------------------------------
# get_tables_with_write_modes
# ---------------------------------------------------------------------------

def _simple_config(tables):
    return {
        "target": {"type": "hyper", "hyper_path": "/tmp/test.hyper"},
        "tables": tables,
    }


class TestGetTablesWithWriteModes:
    def test_returns_7tuple(self):
        config = _simple_config([{
            "table": "Events",
            "write_mode": "append",
            "transformer_mappings": ["log.yaml"],
            "increment_field": "eventTime",
        }])
        rows = get_tables_with_write_modes(config)
        assert len(rows) == 1
        table_name, strategy, drift_policy, mappings, inc_field, sfp, schema = rows[0]
        assert table_name == "Events"
        assert strategy == "incremental"
        assert drift_policy == "rebuild"
        assert mappings == ["log.yaml"]
        assert inc_field == "eventTime"
        assert sfp is None

    def test_replace_table(self):
        config = _simple_config([{
            "table": "Lookup",
            "write_mode": "replace",
            "transformer_mappings": ["lookup.yaml"],
        }])
        _, strategy, drift_policy, *_ = get_tables_with_write_modes(config)[0]
        assert strategy == "replace"
        assert drift_policy == "allow"

    def test_strict_replace_table(self):
        config = _simple_config([{
            "table": "Lookup",
            "write_mode": "strict_replace",
            "transformer_mappings": ["lookup.yaml"],
        }])
        _, strategy, drift_policy, *_ = get_tables_with_write_modes(config)[0]
        assert strategy == "replace"
        assert drift_policy == "fail"

    def test_snapshot_table(self):
        config = _simple_config([{
            "table": "Users",
            "write_mode": "snapshot",
            "snapshot_source_dir": "/tmp/snap",
        }])
        _, strategy, drift_policy, *_ = get_tables_with_write_modes(config)[0]
        assert strategy == "snapshot"
        assert drift_policy == "allow"

    def test_strict_snapshot_table(self):
        config = _simple_config([{
            "table": "Users",
            "write_mode": "strict_snapshot",
            "snapshot_source_dir": "/tmp/snap",
        }])
        _, strategy, drift_policy, *_ = get_tables_with_write_modes(config)[0]
        assert strategy == "snapshot"
        assert drift_policy == "fail"

    def test_schema_default(self):
        config = _simple_config([{
            "table": "T",
            "write_mode": "replace",
            "transformer_mappings": ["m.yaml"],
        }])
        *_, schema = get_tables_with_write_modes(config)[0]
        assert schema == "Extract"

    def test_schema_custom(self):
        config = _simple_config([{
            "table": "T",
            "schema": "snapshots",
            "write_mode": "snapshot",
            "snapshot_source_dir": "/tmp/s",
        }])
        *_, schema = get_tables_with_write_modes(config)[0]
        assert schema == "snapshots"

    def test_empty_tables(self):
        config = _simple_config([])
        assert get_tables_with_write_modes(config) == []


# ---------------------------------------------------------------------------
# get_snapshot_table_configs
# ---------------------------------------------------------------------------

class TestGetSnapshotTableConfigs:
    def test_only_snapshot_tables_returned(self):
        config = _simple_config([
            {"table": "Users", "write_mode": "snapshot", "snapshot_source_dir": "/tmp/snap"},
            {"table": "Events", "write_mode": "append", "transformer_mappings": ["e.yaml"], "increment_field": "t"},
            {"table": "Groups", "write_mode": "strict_snapshot", "snapshot_source_dir": "/tmp/snap"},
        ])
        cfgs = get_snapshot_table_configs(config, "/storage")
        tables = {c["table"] for c in cfgs}
        assert tables == {"Users", "Groups"}

    def test_drift_policy_allow_for_snapshot(self):
        config = _simple_config([{"table": "U", "write_mode": "snapshot", "snapshot_source_dir": "/tmp/snap"}])
        cfgs = get_snapshot_table_configs(config, "/storage")
        assert cfgs[0]["drift_policy"] == "allow"

    def test_drift_policy_fail_for_strict_snapshot(self):
        config = _simple_config([{"table": "U", "write_mode": "strict_snapshot", "snapshot_source_dir": "/tmp/snap"}])
        cfgs = get_snapshot_table_configs(config, "/storage")
        assert cfgs[0]["drift_policy"] == "fail"

    def test_storage_path_substituted(self):
        config = _simple_config([{
            "table": "U",
            "write_mode": "snapshot",
            "snapshot_source_dir": "{storage_path}/transform/snapshots",
        }])
        cfgs = get_snapshot_table_configs(config, "/mydata")
        assert cfgs[0]["snapshot_source_dir"] == "/mydata/transform/snapshots"


# ---------------------------------------------------------------------------
# validate_loader_mapping
# ---------------------------------------------------------------------------

class TestValidateLoaderMapping:
    def _base(self):
        return {"target": {"type": "hyper", "hyper_path": "/tmp/test.hyper"}}

    def test_valid_replace(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "replace", "transformer_mappings": ["m.yaml"]
        }]}
        assert validate_loader_mapping(config) == []

    def test_valid_append(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "append",
            "transformer_mappings": ["m.yaml"], "increment_field": "ts"
        }]}
        assert validate_loader_mapping(config) == []

    def test_valid_snapshot(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "snapshot", "snapshot_source_dir": "/tmp/snap"
        }]}
        assert validate_loader_mapping(config) == []

    def test_valid_strict_snapshot(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "strict_snapshot", "snapshot_source_dir": "/tmp/snap"
        }]}
        assert validate_loader_mapping(config) == []

    def test_invalid_write_mode(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "incremental", "transformer_mappings": ["m.yaml"]
        }]}
        errors = validate_loader_mapping(config)
        assert any("incremental" in e for e in errors)

    def test_append_missing_increment_field(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "append", "transformer_mappings": ["m.yaml"]
        }]}
        errors = validate_loader_mapping(config)
        assert any("increment_field" in e for e in errors)

    def test_strict_append_missing_increment_field(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "strict_append", "transformer_mappings": ["m.yaml"]
        }]}
        errors = validate_loader_mapping(config)
        assert any("increment_field" in e for e in errors)

    def test_snapshot_missing_source_dir(self):
        config = {**self._base(), "tables": [{"table": "T", "write_mode": "snapshot"}]}
        errors = validate_loader_mapping(config)
        assert any("snapshot_source_dir" in e for e in errors)

    def test_snapshot_with_transformer_mappings_errors(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "snapshot",
            "snapshot_source_dir": "/tmp/snap", "transformer_mappings": ["m.yaml"]
        }]}
        errors = validate_loader_mapping(config)
        assert any("snapshot" in e.lower() for e in errors)

    def test_both_transformer_mappings_and_source_file_path_errors(self):
        config = {**self._base(), "tables": [{
            "table": "T", "write_mode": "replace",
            "transformer_mappings": ["m.yaml"], "source_file_path": "/some/dir"
        }]}
        errors = validate_loader_mapping(config)
        assert any("both" in e.lower() or "mutually" in e.lower() or
                   "not both" in e.lower() for e in errors)

    def test_neither_transformer_mappings_nor_source_file_path_errors(self):
        config = {**self._base(), "tables": [{"table": "T", "write_mode": "replace"}]}
        errors = validate_loader_mapping(config)
        assert any("required" in e.lower() or "either" in e.lower() for e in errors)

    def test_no_tables_errors(self):
        config = {**self._base(), "tables": []}
        errors = validate_loader_mapping(config)
        assert errors

    def test_empty_config_errors(self):
        assert validate_loader_mapping({})
