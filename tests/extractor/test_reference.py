"""Tests for the reference data acquisition module (src/extractor/reference.py)."""
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from common.reference_entity import TENANT_SITES, TENANT_USERS
from extractor.reference import run_reference_pass, _utc_now_iso


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeStorage:
    """In-memory storage backend for testing."""

    def __init__(self):
        self._files: dict[str, str] = {}

    def write_file(self, relative_path: str, content: str) -> None:
        self._files[relative_path] = content

    def list_files(self, prefix="") -> list:
        return [k for k in self._files if k.startswith(prefix)]

    def read_json(self, relative_path: str) -> dict:
        return json.loads(self._files[relative_path])


def _make_response(data: dict, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Tests: run_reference_pass for tenant_sites
# ---------------------------------------------------------------------------

class TestRunReferencePassSites:
    def _run(self, pages, storage=None):
        storage = storage or _FakeStorage()
        call_count = [0]

        def fake_request_with_retry(session, method, url, get_headers, reauthenticate,
                                    params=None, **kwargs):
            idx = call_count[0]
            call_count[0] += 1
            return _make_response(pages[idx])

        with patch("extractor.reference.request_with_retry", side_effect=fake_request_with_retry):
            result = run_reference_pass(
                entity=TENANT_SITES,
                api_url="https://example.com",
                tenant_id="tenant-abc",
                session=MagicMock(),
                auth_headers_fn=lambda: {"x-tableau-session-token": "tok"},
                reauth_fn=lambda: True,
                storage_backend=storage,
                op_id="op-test",
                max_results=50,
            )
        return result, storage

    def test_single_page_no_pagination(self):
        pages = [{"sites": [{"siteUUID": "s1"}, {"siteUUID": "s2"}]}]
        result, storage = self._run(pages)

        assert result.entity is TENANT_SITES
        assert result.pages_written == 1
        assert result.total_items == 2
        assert result.error is None
        assert len(result.paths_written) == 1

    def test_page_file_written_as_valid_json(self):
        pages = [{"sites": [{"siteUUID": "s1", "contentUrl": "site1"}]}]
        _, storage = self._run(pages)

        path = list(storage._files.keys())[0]
        data = storage.read_json(path)
        assert data["sites"][0]["siteUUID"] == "s1"

    def test_pagination_loop(self):
        pages = [
            {"sites": [{"siteUUID": "s1"}], "pageToken": "next-token"},
            {"sites": [{"siteUUID": "s2"}, {"siteUUID": "s3"}]},
        ]
        result, storage = self._run(pages)

        assert result.pages_written == 2
        assert result.total_items == 3
        assert len(result.paths_written) == 2

    def test_output_path_contains_pulled_segment(self):
        pages = [{"sites": [{"siteUUID": "s1"}]}]
        result, _ = self._run(pages)

        path = result.paths_written[0]
        assert "reference/tenant_sites/" in path
        assert "/pulled=" in path
        assert "page-" in path

    def test_page_numbered_sequentially(self):
        pages = [
            {"sites": [{"siteUUID": "s1"}], "pageToken": "tok"},
            {"sites": [{"siteUUID": "s2"}]},
        ]
        result, _ = self._run(pages)

        names = [os.path.basename(p) for p in result.paths_written]
        assert "page-0000.json" in names
        assert "page-0001.json" in names

    def test_pull_ts_iso_format(self):
        pages = [{"sites": []}]
        result, _ = self._run(pages)
        # ISO with T separator and Z suffix
        assert "T" in result.pull_ts_iso
        assert result.pull_ts_iso.endswith("Z")

    def test_filesystem_ts_has_no_colons(self):
        pages = [{"sites": []}]
        result, _ = self._run(pages)
        # Colons are invalid in many FS paths; the FS-safe TS replaces them with hyphens
        assert ":" not in result.pull_ts


# ---------------------------------------------------------------------------
# Tests: run_reference_pass for tenant_users
# ---------------------------------------------------------------------------

class TestRunReferencePassUsers:
    def _run(self, pages):
        storage = _FakeStorage()
        call_count = [0]

        def fake_rwr(session, method, url, get_headers, reauthenticate,
                     params=None, **kwargs):
            idx = call_count[0]
            call_count[0] += 1
            return _make_response(pages[idx])

        with patch("extractor.reference.request_with_retry", side_effect=fake_rwr):
            result = run_reference_pass(
                entity=TENANT_USERS,
                api_url="https://example.com",
                tenant_id="tenant-abc",
                session=MagicMock(),
                auth_headers_fn=lambda: {},
                reauth_fn=lambda: True,
                storage_backend=storage,
                op_id="op-test",
            )
        return result, storage

    def test_users_written_verbatim(self):
        user = {"userId": "u1", "email": "a@b.com", "siteRoles": [], "linkRoles": []}
        pages = [{"users": [user]}]
        result, storage = self._run(pages)

        assert result.total_items == 1
        path = result.paths_written[0]
        data = storage.read_json(path)
        assert data["users"][0]["userId"] == "u1"

    def test_all_attributes_preserved_in_raw_json(self):
        user = {
            "userId": "u1", "email": "a@b.com", "userName": "alice",
            "siteRoles": [{"siteId": "s1", "siteRole": "Creator"}],
            "linkRoles": [{"linkId": "l1", "linkRole": "Admin"}],
            "displayName": "Alice", "userStatus": "active",
        }
        pages = [{"users": [user]}]
        _, storage = self._run(pages)
        path = list(storage._files.keys())[0]
        data = storage.read_json(path)
        saved_user = data["users"][0]
        for k in user:
            assert k in saved_user, f"Key {k!r} missing from saved user"
