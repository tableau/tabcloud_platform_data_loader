"""Fixtures for integration / end-to-end tests against a real test tenant.

Every fixture here ultimately depends on ``test_run_config`` (from the
top-level tests/conftest.py), so any test using them SKIPS automatically
when no ``config/run.test.yml`` is present.  Credential resolution goes
through the real ``common.secrets`` resolver, so a missing/locked keyring
entry produces a clean skip rather than a hard failure.
"""

from __future__ import annotations

import os

import pytest

from common.secrets import resolve_secret, SecretResolutionError


@pytest.fixture(scope="session")
def extractor_credentials(test_run_config) -> dict:
    """Resolve the extractor PAT for the test tenant via the secrets resolver.

    Skips (does not fail) if the secret reference cannot be resolved — e.g. the
    keyring entry has not been created yet on this machine.
    """
    conn = test_run_config.connection
    if not conn.token_secret:
        pytest.skip("Integration config missing connection.token_secret")
    try:
        secret = resolve_secret(conn.token_secret)
    except SecretResolutionError as exc:
        pytest.skip(
            f"Could not resolve connection.token_secret ({conn.token_secret}): {exc}. "
            f"Store it with: tabcloud-secrets set tabcloud-test extractor-pat"
        )
    return {
        "pat_name": conn.token_name,
        "pat_secret": secret,
        "api_url": conn.tcm_url,
    }


@pytest.fixture
def temp_storage(tmp_path) -> str:
    """An isolated, per-test storage base path."""
    p = tmp_path / "storage"
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


@pytest.fixture
def runner_env() -> dict:
    """A copy of os.environ suitable for passing to subprocess runs.

    Tests may add entries (e.g. injecting an env: secret) without mutating the
    real process environment.
    """
    return dict(os.environ)
