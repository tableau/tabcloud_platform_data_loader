"""Tests for the per-destination published_through watermark in load_state.py."""

from __future__ import annotations

import json
import os
import pytest

from loader.load_state import (
    get_published_through,
    set_published_through,
    get_last_increment_max,
    set_last_increment_max,
)


class TestPublishedThrough:
    def test_get_returns_none_when_no_state_file(self, tmp_path):
        hyper = str(tmp_path / "data.hyper")
        assert get_published_through(hyper, "prod") is None

    def test_set_and_get_roundtrip(self, tmp_path):
        hyper = str(tmp_path / "data.hyper")
        set_published_through(hyper, "prod", "2026-06-01T00:00:00Z")
        assert get_published_through(hyper, "prod") == "2026-06-01T00:00:00Z"

    def test_separate_destinations_are_independent(self, tmp_path):
        hyper = str(tmp_path / "data.hyper")
        set_published_through(hyper, "prod", "2026-06-01T00:00:00Z")
        set_published_through(hyper, "staging", "2026-06-02T00:00:00Z")
        assert get_published_through(hyper, "prod") == "2026-06-01T00:00:00Z"
        assert get_published_through(hyper, "staging") == "2026-06-02T00:00:00Z"

    def test_unknown_destination_returns_none(self, tmp_path):
        hyper = str(tmp_path / "data.hyper")
        set_published_through(hyper, "prod", "2026-06-01T00:00:00Z")
        assert get_published_through(hyper, "other") is None

    def test_published_through_coexists_with_increment_max(self, tmp_path):
        hyper = str(tmp_path / "data.hyper")
        set_last_increment_max(hyper, "ActivityLog", "2026-06-01T15:00:00Z")
        set_published_through(hyper, "prod", "2026-06-01T16:00:00Z")
        # Both values persisted in same state file
        assert get_last_increment_max(hyper, "ActivityLog") == "2026-06-01T15:00:00Z"
        assert get_published_through(hyper, "prod") == "2026-06-01T16:00:00Z"

    def test_set_overwrites_previous_value(self, tmp_path):
        hyper = str(tmp_path / "data.hyper")
        set_published_through(hyper, "prod", "2026-06-01T00:00:00Z")
        set_published_through(hyper, "prod", "2026-06-02T00:00:00Z")
        assert get_published_through(hyper, "prod") == "2026-06-02T00:00:00Z"

    def test_state_file_is_valid_json(self, tmp_path):
        hyper = str(tmp_path / "data.hyper")
        set_published_through(hyper, "prod", "2026-06-01T00:00:00Z")
        state_path = str(tmp_path / "data_load_state.json")
        with open(state_path, encoding="utf-8") as f:
            data = json.load(f)
        assert isinstance(data, dict)
        assert "published_through__prod" in data
