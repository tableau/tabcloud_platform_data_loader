"""Tests for runner secrets handling: _pass_secret, _run_component env merging.

These tests exercise the helpers that prevent plaintext secrets from appearing
in subprocess argv.  They are independent of the YAML config model.
"""

from __future__ import annotations

import os
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from runner.cli import _pass_secret, _run_component


# ---------------------------------------------------------------------------
# _pass_secret
# ---------------------------------------------------------------------------


class TestPassSecret:
    def test_keyring_ref_appended_to_args(self):
        args: List[str] = []
        env: dict = {}
        _pass_secret(args, env, "--pat-secret-ref", "keyring:tabcloud/extractor-pat", "_PAT_ENV")
        assert args == ["--pat-secret-ref", "keyring:tabcloud/extractor-pat"]
        assert "_PAT_ENV" not in env

    def test_env_ref_appended_to_args(self):
        args: List[str] = []
        env: dict = {}
        _pass_secret(args, env, "--pat-secret-ref", "env:MY_PAT", "_PAT_ENV")
        assert args == ["--pat-secret-ref", "env:MY_PAT"]
        assert "_PAT_ENV" not in env

    def test_file_ref_appended_to_args(self):
        args: List[str] = []
        env: dict = {}
        _pass_secret(args, env, "--pat-secret-ref", "file:/tmp/secret.txt", "_PAT_ENV")
        assert args == ["--pat-secret-ref", "file:/tmp/secret.txt"]

    def test_awssm_ref_appended_to_args(self):
        args: List[str] = []
        env: dict = {}
        _pass_secret(args, env, "--pat-secret-ref", "awssm:secret/id#KEY", "_PAT_ENV")
        assert args == ["--pat-secret-ref", "awssm:secret/id#KEY"]

    def test_plain_ref_appended_to_args(self):
        args: List[str] = []
        env: dict = {}
        _pass_secret(args, env, "--pat-secret-ref", "plain:literal-value", "_PAT_ENV")
        assert args == ["--pat-secret-ref", "plain:literal-value"]
        assert "_PAT_ENV" not in env

    def test_bare_literal_injected_into_env(self):
        args: List[str] = []
        env: dict = {}
        secret = "super-secret-token-xyz"
        _pass_secret(args, env, "--pat-secret-ref", secret, "_PAT_ENV")
        assert secret not in args
        assert env.get("_PAT_ENV") == secret
        assert args == ["--pat-secret-ref", "env:_PAT_ENV"]

    def test_literal_never_in_argv_as_plaintext(self):
        """Core security guarantee: a literal secret value must not appear in args."""
        args: List[str] = []
        env: dict = {}
        secret = "my-super-secret-12345"
        _pass_secret(args, env, "--secret-ref", secret, "_SEC")
        for arg in args:
            assert secret not in arg


# ---------------------------------------------------------------------------
# _run_component: env merging
# ---------------------------------------------------------------------------


class TestRunComponentEnv:
    def test_env_merged_over_os_environ(self):
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["env"] = kwargs.get("env", {})
            return MagicMock(returncode=0)

        with patch("runner.cli.subprocess.run", side_effect=fake_run):
            _run_component("extractor", ["--help"], cwd="/tmp", env={"_MY_KEY": "myval"})

        assert "_MY_KEY" in captured["env"]
        assert captured["env"]["_MY_KEY"] == "myval"
        assert "PATH" in captured["env"] or len(captured["env"]) > 1

    def test_no_env_uses_os_environ(self):
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["env"] = kwargs.get("env", {})
            return MagicMock(returncode=0)

        with patch("runner.cli.subprocess.run", side_effect=fake_run):
            _run_component("extractor", ["--help"], cwd="/tmp")

        assert len(captured["env"]) > 0

    def test_command_includes_python_dash_m(self):
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            return MagicMock(returncode=0)

        with patch("runner.cli.subprocess.run", side_effect=fake_run):
            _run_component("extractor", ["--help"], cwd="/tmp")

        assert "-m" in captured["cmd"]
        assert "extractor" in captured["cmd"]
