"""End-to-end pipeline tests driving the real runner against the test tenant.

These invoke the actual ``python -m runner --config <config.test.ini>`` entry
point as a subprocess — the same path real users take — which exercises:
  - secret-reference resolution (keyring:/env:) and the no-plaintext-argv passing,
  - extractor → transformer → loader orchestration,
  - your custom test mappings in tests/integration/mappings/.

Run:
    pytest tests/integration -m integration -k e2e -v -s

All tests SKIP when config/config.test.ini is absent (see tests/conftest.py).
"""

from __future__ import annotations

import glob
import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.integration


def _resolve_storage_path(test_config, test_config_path) -> str:
    """Replicate the runner's rule: paths are relative to the config file's folder."""
    raw = test_config.get("paths", "storage_path", fallback="./data_workspace").strip()
    if os.path.isabs(raw):
        return raw
    config_dir = os.path.dirname(os.path.abspath(test_config_path))
    return os.path.abspath(os.path.join(config_dir, raw))


def test_e2e_runner_pipeline(test_config, test_config_path, extractor_credentials):
    """Full runner pipeline produces a .hyper file with no errors.

    Depends on ``extractor_credentials`` so it skips when the secret is missing,
    even though the runner resolves the secret itself — this gives a clean skip
    instead of a subprocess failure.
    """
    storage_path = _resolve_storage_path(test_config, test_config_path)

    result = subprocess.run(
        [sys.executable, "-m", "runner", "--config", test_config_path],
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        capture_output=True,
        text=True,
        timeout=900,
    )

    # Surface logs on failure to make debugging easy.
    if result.returncode != 0:
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)

    assert result.returncode == 0, (
        f"Runner exited {result.returncode}. See captured output above. "
        f"Common causes: expired PAT, wrong server_url/site, or no data in the window."
    )

    loader_dir = os.path.join(storage_path, "loader")
    hyper_files = glob.glob(os.path.join(loader_dir, "*.hyper"))
    assert hyper_files, (
        f"No .hyper file produced under {loader_dir}. "
        f"Check the loader mapping and that the transformer produced output."
    )


def test_e2e_no_plaintext_secret_in_logs(test_config, test_config_path, extractor_credentials):
    """The resolved secret value must never appear in runner stdout/stderr."""
    result = subprocess.run(
        [sys.executable, "-m", "runner", "--config", test_config_path],
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        capture_output=True,
        text=True,
        timeout=900,
    )
    secret = extractor_credentials["pat_secret"]
    combined = (result.stdout or "") + (result.stderr or "")
    assert secret not in combined, "Resolved PAT secret leaked into runner output!"
