"""Tests for the reference data normalization pipeline (_run_reference_pipeline)."""
import json
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

pytestmark = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_pages(extract_dir: str, entity_name: str, pulled_ts: str, pages: list[dict]):
    """Write page JSON files under extract/reference/{entity}/pulled={ts}/."""
    base = os.path.join(extract_dir, "reference", entity_name, f"pulled={pulled_ts}")
    os.makedirs(base, exist_ok=True)
    for i, page in enumerate(pages):
        with open(os.path.join(base, f"page-{i:04d}.json"), "w") as f:
            json.dump(page, f)


class _DirStorage:
    """Minimal StorageBackend that delegates to a real directory."""

    def __init__(self, base_path: str):
        self.base_path = base_path

    def list_files(self, prefix="") -> list:
        root = os.path.join(self.base_path, prefix)
        files = []
        for dirpath, _, fnames in os.walk(root):
            for fname in fnames:
                full = os.path.join(dirpath, fname)
                files.append(os.path.relpath(full, self.base_path).replace("\\", "/"))
        return files

    def full_path(self, relative_path: str) -> str:
        return os.path.join(self.base_path, relative_path)


# ---------------------------------------------------------------------------
# Import the function under test
# ---------------------------------------------------------------------------

from transformer.pipeline import _run_reference_pipeline, _normalize_pull_ts


# ---------------------------------------------------------------------------
# Tests: _normalize_pull_ts
# ---------------------------------------------------------------------------

class TestNormalizePullTs:
    def test_restores_colons(self):
        assert _normalize_pull_ts("2026-06-15T08-30-00Z") == "2026-06-15T08:30:00Z"

    def test_no_T_returns_as_is(self):
        assert _normalize_pull_ts("2026-06-15") == "2026-06-15"

    def test_only_first_two_hyphens_replaced(self):
        # "08-30-00" → "08:30:00"
        result = _normalize_pull_ts("2026-06-15T08-30-00Z")
        assert result == "2026-06-15T08:30:00Z"


# ---------------------------------------------------------------------------
# Tests: _run_reference_pipeline for tenant_sites
# ---------------------------------------------------------------------------

class TestReferencePipelineSites:
    def _run(self, pages, pulled_ts="2026-06-15T10-00-00Z"):
        with tempfile.TemporaryDirectory() as tmp:
            _write_pages(tmp, "tenant_sites", pulled_ts, pages)
            storage = _DirStorage(tmp)
            out_dir = os.path.join(tmp, "transform", "reference")
            config = {
                "reference_mode": True,
                "output_filename": "tenant_reference",
                "reference_entities": [
                    {"name": "tenant_sites", "items_key": "sites", "table_name": "tenant_sites"},
                ],
            }
            written, fmt = _run_reference_pipeline("op-1", storage, config, out_dir, "tenant_reference")
            # Read back before tmp dir is deleted
            parquet_path = os.path.join(out_dir, "tenant_sites.parquet")
            manifest_path = os.path.join(out_dir, "_manifest.json")
            rows = None
            manifest = None
            if os.path.exists(parquet_path):
                rows = pq.read_table(parquet_path).to_pydict()
            if os.path.exists(manifest_path):
                with open(manifest_path) as f:
                    manifest = json.load(f)
        return written, fmt, rows, manifest

    def test_parquet_written(self):
        pages = [{"sites": [{"siteUUID": "uuid-1", "contentUrl": "site1", "status": "Active"}]}]
        written, fmt, rows, _ = self._run(pages)
        assert fmt == "parquet"
        assert any("tenant_sites.parquet" in w for w in written)
        assert rows is not None

    def test_site_luid_aliased(self):
        pages = [{"sites": [{"siteUUID": "uuid-abc", "contentUrl": "site1", "status": "Active"}]}]
        _, _, rows, _ = self._run(pages)
        assert "siteLuid" in rows
        assert rows["siteLuid"][0] == "uuid-abc"

    def test_all_attributes_retained(self):
        site = {"siteUUID": "u1", "contentUrl": "c1", "status": "Active", "suspendedDateTime": None, "extra": "val"}
        pages = [{"sites": [site]}]
        _, _, rows, _ = self._run(pages)
        assert "extra" in rows
        assert rows["extra"][0] == "val"

    def test_nested_dict_attribute_serialized_to_json_string(self):
        """Nested dict fields (e.g. 'instance' from TCM) must become JSON strings, not structs."""
        instance = {
            "id": "inst-1",
            "baseUrl": "https://example.com",
            "serverInstanceId": "si-1",
            "regionUUID": "r1",
            "cloudProvider": {"name": "Azure", "region": "westus"},
        }
        site = {"siteUUID": "u1", "contentUrl": "site1", "status": "Active", "instance": instance}
        pages = [{"sites": [site]}]
        _, _, rows, _ = self._run(pages)
        assert "instance" in rows
        val = rows["instance"][0]
        assert isinstance(val, str), "nested dict must be serialized to a JSON string"
        parsed = json.loads(val)
        assert parsed["id"] == "inst-1"
        assert parsed["cloudProvider"]["name"] == "Azure"

    def test_nested_list_attribute_serialized_to_json_string(self):
        """List-valued attributes must also become JSON strings."""
        tags = ["infra", "prod"]
        site = {"siteUUID": "u1", "contentUrl": "site1", "status": "Active", "tags": tags}
        pages = [{"sites": [site]}]
        _, _, rows, _ = self._run(pages)
        assert "tags" in rows
        val = rows["tags"][0]
        assert isinstance(val, str), "list attribute must be serialized to a JSON string"
        assert json.loads(val) == tags

    def test_real_tcm_shape_no_struct_column(self):
        """Full TCM-shaped site record (instance struct + cloudProvider nested) writes as all scalars."""
        site = {
            "tenantId": "t1",
            "siteUUID": "uuid-1",
            "name": "Test Site",
            "contentUrl": "testsite",
            "instance": {
                "id": "inst-1",
                "baseUrl": "https://example.com",
                "serverInstanceId": "srv-1",
                "regionUUID": "rg-1",
                "cloudProvider": {"name": "AWS", "region": "us-east-1"},
            },
            "creatorCapacity": 60,
            "explorerCapacity": 1000,
            "viewerCapacity": 1000,
            "location": "loc-uuid",
            "storageQuota": 10000000,
            "storageUsed": 5,
            "status": "Active",
            "createDateTime": "2026-05-01T00:00:00Z",
            "lastUpdateDateTime": "2026-06-14T00:00:00Z",
        }
        pages = [{"sites": [site]}]
        with tempfile.TemporaryDirectory() as tmp:
            _write_pages(tmp, "tenant_sites", "2026-06-15T10-00-00Z", pages)
            storage = _DirStorage(tmp)
            out_dir = os.path.join(tmp, "transform", "reference")
            config = {
                "reference_mode": True,
                "reference_entities": [
                    {"name": "tenant_sites", "items_key": "sites", "table_name": "tenant_sites"},
                ],
            }
            _run_reference_pipeline("op-1", storage, config, out_dir, "tenant_reference")
            tbl = pq.read_table(os.path.join(out_dir, "tenant_sites.parquet"))
            # Every column must be a non-struct type (scalars or strings)
            for field in tbl.schema:
                assert not pq.ParquetFile(
                    os.path.join(out_dir, "tenant_sites.parquet")
                ).schema_arrow.field(field.name).type.__class__.__name__ == "StructType", (
                    f"Column {field.name!r} must not be a struct"
                )
            rows = tbl.to_pydict()
        assert rows["siteLuid"][0] == "uuid-1"
        assert isinstance(rows["instance"][0], str)
        assert json.loads(rows["instance"][0])["cloudProvider"]["name"] == "AWS"

    def test_manifest_written_with_snapshot_time(self):
        pages = [{"sites": [{"siteUUID": "u1"}]}]
        _, _, _, manifest = self._run(pages, pulled_ts="2026-06-15T10-00-00Z")
        assert manifest is not None
        assert "tenant_sites" in manifest
        assert manifest["tenant_sites"]["snapshot_time"] == "2026-06-15T10:00:00Z"

    def test_multiple_pages_unioned(self):
        pages = [
            {"sites": [{"siteUUID": "s1"}]},
            {"sites": [{"siteUUID": "s2"}, {"siteUUID": "s3"}]},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            _write_pages(tmp, "tenant_sites", "2026-06-15T10-00-00Z", pages)
            storage = _DirStorage(tmp)
            out_dir = os.path.join(tmp, "transform", "reference")
            config = {
                "reference_mode": True,
                "reference_entities": [
                    {"name": "tenant_sites", "items_key": "sites", "table_name": "tenant_sites"},
                ],
            }
            _run_reference_pipeline("op-1", storage, config, out_dir, "tenant_reference")
            rows = pq.read_table(os.path.join(out_dir, "tenant_sites.parquet")).to_pydict()
        assert len(rows["siteLuid"]) == 3

    def test_no_data_skips_without_error(self):
        pages = [{"sites": []}]
        written, _, rows, _ = self._run(pages)
        assert written == []
        assert rows is None

    def test_newest_pulled_dir_used(self):
        """When multiple pulled= dirs exist, the newest is used."""
        with tempfile.TemporaryDirectory() as tmp:
            _write_pages(tmp, "tenant_sites", "2026-06-15T08-00-00Z",
                         [{"sites": [{"siteUUID": "old"}]}])
            _write_pages(tmp, "tenant_sites", "2026-06-15T10-00-00Z",
                         [{"sites": [{"siteUUID": "new"}]}])
            storage = _DirStorage(tmp)
            out_dir = os.path.join(tmp, "transform", "reference")
            config = {
                "reference_mode": True,
                "reference_entities": [
                    {"name": "tenant_sites", "items_key": "sites", "table_name": "tenant_sites"},
                ],
            }
            _run_reference_pipeline("op-1", storage, config, out_dir, "tenant_reference")
            rows = pq.read_table(os.path.join(out_dir, "tenant_sites.parquet")).to_pydict()
        assert rows["siteLuid"][0] == "new"


# ---------------------------------------------------------------------------
# Tests: _run_reference_pipeline for tenant_users (membership grain)
# ---------------------------------------------------------------------------

class TestReferencePipelineUsers:
    def _make_user(self, user_id, site_roles=None, link_roles=None, **kwargs):
        return {
            "userId": user_id,
            "email": f"{user_id}@example.com",
            "userName": user_id,
            "displayName": user_id.capitalize(),
            "userStatus": "active",
            "maxSiteRole": "Creator",
            "tcmRoleName": "Creator",
            "numberOfSiteRoles": len(site_roles or []),
            "lastLoginTime": None,
            "language": "en",
            "locale": "en_US",
            "tenantId": "t1",
            "siteRoles": site_roles or [],
            "linkRoles": link_roles or [],
            **kwargs,
        }

    def _run(self, users, pulled_ts="2026-06-15T10-00-00Z"):
        pages = [{"users": users}]
        with tempfile.TemporaryDirectory() as tmp:
            _write_pages(tmp, "tenant_users", pulled_ts, pages)
            storage = _DirStorage(tmp)
            out_dir = os.path.join(tmp, "transform", "reference")
            config = {
                "reference_mode": True,
                "reference_entities": [
                    {"name": "tenant_users", "items_key": "users", "table_name": "tenant_users"},
                ],
            }
            _run_reference_pipeline("op-1", storage, config, out_dir, "tenant_reference")
            parquet_path = os.path.join(out_dir, "tenant_users.parquet")
            manifest_path = os.path.join(out_dir, "_manifest.json")
            rows = pq.read_table(parquet_path).to_pydict() if os.path.exists(parquet_path) else None
            manifest = json.load(open(manifest_path)) if os.path.exists(manifest_path) else None
        return rows, manifest

    def test_user_luid_aliased(self):
        users = [self._make_user("u1", site_roles=[{"siteId": "s1", "siteRole": "Creator"}])]
        rows, _ = self._run(users)
        assert "userLuid" in rows
        assert "u1" in rows["userLuid"]

    def test_site_luid_aliased_from_site_roles(self):
        users = [self._make_user("u1", site_roles=[{"siteId": "site-abc", "siteRole": "Creator"}])]
        rows, _ = self._run(users)
        assert "siteLuid" in rows
        assert "site-abc" in rows["siteLuid"]

    def test_membership_grain_one_row_per_site_role(self):
        users = [self._make_user("u1", site_roles=[
            {"siteId": "s1", "siteRole": "Creator"},
            {"siteId": "s2", "siteRole": "Viewer"},
        ])]
        rows, _ = self._run(users)
        assert rows["userLuid"].count("u1") == 2
        assert set(rows["siteLuid"]) == {"s1", "s2"}

    def test_user_with_no_site_roles_emits_one_null_row(self):
        users = [self._make_user("u1", site_roles=[])]
        rows, _ = self._run(users)
        assert len(rows["userLuid"]) == 1
        assert rows["userLuid"][0] == "u1"
        assert rows["siteLuid"][0] is None

    def test_multiple_users(self):
        users = [
            self._make_user("u1", site_roles=[{"siteId": "s1", "siteRole": "Creator"}]),
            self._make_user("u2", site_roles=[
                {"siteId": "s1", "siteRole": "Viewer"},
                {"siteId": "s2", "siteRole": "SiteAdministratorCreator"},
            ]),
        ]
        rows, _ = self._run(users)
        assert len(rows["userLuid"]) == 3  # u1 x 1 site + u2 x 2 sites

    def test_link_roles_serialized_as_json_string(self):
        link_roles = [{"linkId": "l1", "linkRole": "Admin", "linkRoleStatus": "Active"}]
        users = [self._make_user("u1", site_roles=[], link_roles=link_roles)]
        rows, _ = self._run(users)
        parsed = json.loads(rows["linkRoles"][0])
        assert parsed[0]["linkId"] == "l1"

    def test_user_level_attributes_retained(self):
        users = [self._make_user("u1", site_roles=[{"siteId": "s1", "siteRole": "Creator"}])]
        rows, _ = self._run(users)
        for col in ("email", "userName", "displayName", "userStatus", "maxSiteRole",
                    "tcmRoleName", "tenantId", "language", "locale"):
            assert col in rows, f"Column {col!r} missing"

    def test_manifest_snapshot_time(self):
        users = [self._make_user("u1", site_roles=[])]
        _, manifest = self._run(users, pulled_ts="2026-06-15T10-00-00Z")
        assert manifest["tenant_users"]["snapshot_time"] == "2026-06-15T10:00:00Z"

    def test_both_entities_in_single_pass(self):
        """Both tenant_sites and tenant_users can be produced in one pipeline invocation."""
        with tempfile.TemporaryDirectory() as tmp:
            _write_pages(tmp, "tenant_sites", "2026-06-15T10-00-00Z",
                         [{"sites": [{"siteUUID": "s1"}]}])
            _write_pages(tmp, "tenant_users", "2026-06-15T10-00-00Z",
                         [{"users": [self._make_user("u1", site_roles=[{"siteId": "s1", "siteRole": "Creator"}])]}])
            storage = _DirStorage(tmp)
            out_dir = os.path.join(tmp, "transform", "reference")
            config = {
                "reference_mode": True,
                "reference_entities": [
                    {"name": "tenant_sites", "items_key": "sites", "table_name": "tenant_sites"},
                    {"name": "tenant_users", "items_key": "users", "table_name": "tenant_users"},
                ],
            }
            written, _ = _run_reference_pipeline("op-1", storage, config, out_dir, "tenant_reference")
            sites_rows = pq.read_table(os.path.join(out_dir, "tenant_sites.parquet")).to_pydict()
            users_rows = pq.read_table(os.path.join(out_dir, "tenant_users.parquet")).to_pydict()
            manifest = json.load(open(os.path.join(out_dir, "_manifest.json")))

        assert len(written) == 2
        assert sites_rows["siteLuid"][0] == "s1"
        assert users_rows["userLuid"][0] == "u1"
        assert "tenant_sites" in manifest
        assert "tenant_users" in manifest
