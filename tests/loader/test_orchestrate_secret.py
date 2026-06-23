"""
Tests for orchestrate.run_extractor secret-passing behaviour.

Ensures the PAT secret is injected into the child environment and
does NOT appear on the argv command line (visible via ``ps``).
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

SRC = os.path.join(os.path.dirname(__file__), "..", "..", "src")
sys.path.insert(0, os.path.abspath(SRC))

from loader.orchestrate import run_extractor, _ORCHESTRATE_PAT_ENV


class TestRunExtractorSecretPassing(unittest.TestCase):

    def _capture_run(self, **kwargs):
        """Call run_extractor and capture the subprocess.run call arguments."""
        captured = {}

        def fake_run(cmd, *, cwd, env):
            captured["cmd"] = cmd
            captured["env"] = env
            result = MagicMock()
            result.returncode = 0
            return result

        with patch("loader.orchestrate.subprocess.run", side_effect=fake_run):
            run_extractor(**kwargs)

        return captured

    def _base_params(self, secret="super-secret-pat"):
        return dict(
            storage_path="/tmp/storage",
            pat_name="my-pat-name",
            pat_secret=secret,
            api_url="https://example.online.tableau.com",
            start_time="2026-01-01T00:00:00Z",
            end_time="2026-01-02T00:00:00Z",
        )

    def test_secret_not_in_argv(self):
        """The resolved PAT secret must never appear in the subprocess command list."""
        secret = "super-secret-pat-value-12345"
        captured = self._capture_run(**self._base_params(secret=secret))
        cmd_str = " ".join(captured["cmd"])
        self.assertNotIn(secret, cmd_str, "PAT secret must not appear in subprocess argv")

    def test_secret_injected_into_child_env(self):
        """The secret must be placed in the child environment under _ORCHESTRATE_PAT_ENV."""
        secret = "env-injected-secret-xyz"
        captured = self._capture_run(**self._base_params(secret=secret))
        self.assertIn(_ORCHESTRATE_PAT_ENV, captured["env"])
        self.assertEqual(captured["env"][_ORCHESTRATE_PAT_ENV], secret)

    def test_pat_secret_ref_flag_used(self):
        """The command must use --pat-secret-ref, not --pat-secret."""
        captured = self._capture_run(**self._base_params())
        self.assertIn("--pat-secret-ref", captured["cmd"])
        self.assertNotIn("--pat-secret", captured["cmd"])

    def test_pat_secret_ref_value_is_env_reference(self):
        """The value passed for --pat-secret-ref must reference the env var, not the literal."""
        captured = self._capture_run(**self._base_params())
        idx = captured["cmd"].index("--pat-secret-ref")
        ref_value = captured["cmd"][idx + 1]
        self.assertTrue(
            ref_value.startswith("env:"),
            f"Expected env: reference, got: {ref_value!r}",
        )
        self.assertIn(_ORCHESTRATE_PAT_ENV, ref_value)

    def test_child_env_inherits_parent_environ(self):
        """Child env must include parent os.environ entries in addition to the injected secret."""
        with patch.dict(os.environ, {"_TEST_PARENT_VAR": "parent-value"}):
            captured = self._capture_run(**self._base_params())
        self.assertIn("_TEST_PARENT_VAR", captured["env"])
        self.assertEqual(captured["env"]["_TEST_PARENT_VAR"], "parent-value")

    def test_exit_code_returned(self):
        def fake_run(cmd, *, cwd, env):
            result = MagicMock()
            result.returncode = 2
            return result

        with patch("loader.orchestrate.subprocess.run", side_effect=fake_run):
            rc = run_extractor(**self._base_params())
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
