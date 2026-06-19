"""Shared pytest configuration and fixtures for the whole test suite.

Defines the ``integration`` marker and a session-scoped ``test_run_config``
fixture that loads ``config/run.test.yml`` (or the path in the
``TABCLOUD_TEST_CONFIG`` env var).  Integration tests that depend on a real
test tenant should request ``test_run_config`` (or fixtures derived from it)
so they SKIP gracefully when no test environment is configured — the default
``pytest`` run stays green on a clean checkout with no credentials.
"""

from __future__ import annotations

import os

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: test requires a live test tenant + stored credentials "
        "(see tests/integration/README.md). Excluded from the default run.",
    )


def _repo_root() -> str:
    # tests/ -> repo root
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="session")
def repo_root() -> str:
    """Absolute path to the repository root."""
    return _repo_root()


@pytest.fixture(scope="session")
def test_config_path() -> str:
    """Resolved path to the integration run.yml config.

    Override with the ``TABCLOUD_TEST_CONFIG`` environment variable; otherwise
    defaults to ``config/run.test.yml`` at the repo root.
    """
    override = os.environ.get("TABCLOUD_TEST_CONFIG")
    if override:
        return os.path.abspath(override)
    return os.path.join(_repo_root(), "config", "run.test.yml")


@pytest.fixture(scope="session")
def test_run_config(test_config_path):
    """Load the integration run.yml config, or SKIP if it does not exist.

    This is the single gate that makes integration tests opt-in: with no
    ``config/run.test.yml`` present, every test that requests this fixture
    is skipped rather than failed.
    """
    if not os.path.isfile(test_config_path):
        pytest.skip(
            f"No integration config at {test_config_path}. "
            f"Copy config/run.test.yml.example to config/run.test.yml "
            f"(or set TABCLOUD_TEST_CONFIG) and store credentials with "
            f"'tabcloud-secrets set tabcloud-test extractor-pat'."
        )
    from runner.config import load_run_config, RunConfigError
    try:
        return load_run_config(test_config_path)
    except RunConfigError as exc:
        pytest.skip(f"Integration config could not be parsed: {exc}")
