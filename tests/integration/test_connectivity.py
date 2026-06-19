"""Connectivity smoke tests against the live test tenant.

These verify that the credentials stored in the OS keychain actually authenticate
against the configured test tenant. They are the fastest way to confirm your test
environment is wired up correctly before running heavier E2E tests.

Run:
    pytest tests/integration -m integration -k connectivity -v

All tests SKIP automatically when config/config.test.ini is absent or the secret
cannot be resolved (see tests/conftest.py and tests/integration/conftest.py).
"""

from __future__ import annotations

import pytest

from extractor.retriever import LogRetriever, create_operation_id
from common.dataset_profile import ACTIVITYLOG
from storage.local import LocalStorage

pytestmark = pytest.mark.integration


def _make_retriever(creds: dict, storage_path: str) -> LogRetriever:
    return LogRetriever(
        pat_name=creds["pat_name"],
        pat_secret=creds["pat_secret"],
        api_url=creds["api_url"],
        site_ids=[],
        start_time="",
        end_time="",
        storage_backend=LocalStorage(base_path=storage_path),
        dataset_profile=ACTIVITYLOG,
    )


def test_authenticate(extractor_credentials, temp_storage):
    """The stored PAT logs in and yields a session token."""
    retriever = _make_retriever(extractor_credentials, temp_storage)
    ok = retriever._login(create_operation_id())
    assert ok, (
        "Login failed against the test tenant. Verify the PAT name/secret and "
        "server_url in config.test.ini, and that the PAT has not expired."
    )
    assert retriever.session_token, "Expected a non-empty session token after login."


def test_list_sites(extractor_credentials, temp_storage):
    """The authenticated session can enumerate tenant sites."""
    retriever = _make_retriever(extractor_credentials, temp_storage)
    assert retriever._login(create_operation_id()), "Login failed; cannot list sites."
    sites = retriever._list_tenant_sites(create_operation_id())
    # A healthy tenant returns at least one site; assert the call returns a list.
    assert isinstance(sites, list), "Expected a list of sites from the tenant."
