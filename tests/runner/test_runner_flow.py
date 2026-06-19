"""
Tests for the YAML-based phase engine in runner.cli.

Covers:
  - Phase ordering and fan-out (all siblings run in a phase).
  - Halt-at-barrier: extraction failure prevents transform and load from running.
  - Halt-at-barrier: transform failure prevents load from running.
  - Load failure does not cascade back (post-barrier).
  - Degraded exit (2) from extraction does not halt.
  - _pass_secret: pointer refs passed as-is; bare literals injected into env.
  - _run_component: env merging.
"""

from __future__ import annotations

import os
import sys
from typing import List
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

import runner.cli as cli
from runner.config import (
    RunConfig,
    ConnectionConfig,
    ExtractionJobConfig,
    TransformJobConfig,
    LoadJobConfig,
    StorageConfig,
    LoggingConfig,
    DATA_EVENT_LOGS,
    DATA_SNAPSHOTS,
    bundled_mappings_root,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(
    extraction=None,
    transform=None,
    load=None,
    destinations=None,
) -> RunConfig:
    """Build a minimal RunConfig for testing. Phases can be set or defaulted empty."""
    return RunConfig(
        version=1,
        base_dir="/tmp/project",
        connection=ConnectionConfig(
            tcm_url="https://example.online.tableau.com",
            token_name="test-pat",
            token_secret="keyring:tabcloud/extractor-pat",
        ),
        extraction=extraction or {},
        transform=transform or {},
        load=load or {},
        destinations=destinations or {},
        storage=StorageConfig(path="/tmp/data"),
        logging_cfg=LoggingConfig(),
    )


def _el_job(name="events") -> ExtractionJobConfig:
    return ExtractionJobConfig(name=name, data=DATA_EVENT_LOGS)


def _snap_job(name="snapshots") -> ExtractionJobConfig:
    return ExtractionJobConfig(name=name, data=DATA_SNAPSHOTS)


def _tx_job(name="events", mapping="/tmp/mappings/event.yaml") -> TransformJobConfig:
    return TransformJobConfig(name=name, mapping=mapping)


def _load_job(name="default", mapping="/tmp/mappings/loader.yml") -> LoadJobConfig:
    return LoadJobConfig(name=name, mapping=mapping)


# ---------------------------------------------------------------------------
# Phase ordering
# ---------------------------------------------------------------------------


class TestPhaseOrdering:
    def test_all_phases_run_when_all_succeed(self):
        cfg = _make_cfg(
            extraction={"events": _el_job()},
            transform={"t": _tx_job()},
            load={"l": _load_job()},
        )
        calls: List[str] = []

        with (
            patch.object(cli, "run_extraction_job", side_effect=lambda *a, **kw: calls.append("extract") or 0),
            patch.object(cli, "run_transform_job",  side_effect=lambda *a, **kw: calls.append("transform") or 0),
            patch.object(cli, "run_load_job",        side_effect=lambda *a, **kw: calls.append("load") or 0),
        ):
            rc = cli._execute_phases(cfg, [])

        assert rc == 0
        assert calls == ["extract", "transform", "load"]

    def test_extraction_failure_halts_at_barrier(self):
        cfg = _make_cfg(
            extraction={"events": _el_job()},
            transform={"t": _tx_job()},
            load={"l": _load_job()},
        )
        calls: List[str] = []

        with (
            patch.object(cli, "run_extraction_job", side_effect=lambda *a, **kw: calls.append("extract") or 1),
            patch.object(cli, "run_transform_job",  side_effect=lambda *a, **kw: calls.append("transform") or 0),
            patch.object(cli, "run_load_job",        side_effect=lambda *a, **kw: calls.append("load") or 0),
        ):
            rc = cli._execute_phases(cfg, [])

        assert rc == 1
        assert "extract" in calls
        assert "transform" not in calls
        assert "load" not in calls

    def test_transform_failure_halts_at_barrier(self):
        cfg = _make_cfg(
            extraction={"events": _el_job()},
            transform={"t": _tx_job()},
            load={"l": _load_job()},
        )
        calls: List[str] = []

        with (
            patch.object(cli, "run_extraction_job", side_effect=lambda *a, **kw: calls.append("extract") or 0),
            patch.object(cli, "run_transform_job",  side_effect=lambda *a, **kw: calls.append("transform") or 1),
            patch.object(cli, "run_load_job",        side_effect=lambda *a, **kw: calls.append("load") or 0),
        ):
            rc = cli._execute_phases(cfg, [])

        assert rc == 1
        assert "transform" in calls
        assert "load" not in calls

    def test_load_failure_returns_nonzero(self):
        cfg = _make_cfg(
            extraction={"events": _el_job()},
            transform={"t": _tx_job()},
            load={"l": _load_job()},
        )
        with (
            patch.object(cli, "run_extraction_job", return_value=0),
            patch.object(cli, "run_transform_job",  return_value=0),
            patch.object(cli, "run_load_job",        return_value=1),
        ):
            rc = cli._execute_phases(cfg, [])

        assert rc == 1

    def test_degraded_extraction_does_not_halt(self):
        """Exit code 2 (degraded) from extraction should not stop transform."""
        cfg = _make_cfg(
            extraction={"events": _el_job()},
            transform={"t": _tx_job()},
            load={"l": _load_job()},
        )
        calls: List[str] = []

        with (
            patch.object(cli, "run_extraction_job", side_effect=lambda *a, **kw: calls.append("extract") or 2),
            patch.object(cli, "run_transform_job",  side_effect=lambda *a, **kw: calls.append("transform") or 0),
            patch.object(cli, "run_load_job",        side_effect=lambda *a, **kw: calls.append("load") or 0),
        ):
            rc = cli._execute_phases(cfg, [])

        assert rc == 0
        assert calls == ["extract", "transform", "load"]


# ---------------------------------------------------------------------------
# Fan-out: all siblings in a phase run before the barrier
# ---------------------------------------------------------------------------


class TestFanOut:
    def test_all_extraction_jobs_run_before_barrier(self):
        """Even when the first extraction job fails, its sibling must still run."""
        cfg = _make_cfg(
            extraction={
                "a": _el_job("a"),
                "b": _snap_job("b"),
            },
            transform={"t": _tx_job()},
        )
        calls: List[str] = []
        results = {"a": 1, "b": 0}

        def _fake_extract(cfg_, name, job, storage):
            calls.append(name)
            return results[name]

        with (
            patch.object(cli, "run_extraction_job", side_effect=_fake_extract),
            patch.object(cli, "run_transform_job",  return_value=0),
        ):
            rc = cli._execute_phases(cfg, [])

        assert "a" in calls
        assert "b" in calls  # sibling ran despite 'a' failing
        assert rc == 1        # barrier halts

    def test_all_transform_jobs_run_before_barrier(self):
        cfg = _make_cfg(
            transform={"t1": _tx_job("t1"), "t2": _tx_job("t2")},
            load={"l": _load_job()},
        )
        calls: List[str] = []
        results = {"t1": 1, "t2": 0}

        def _fake_transform(cfg_, name, job, storage):
            calls.append(name)
            return results[name]

        with (
            patch.object(cli, "run_transform_job", side_effect=_fake_transform),
            patch.object(cli, "run_load_job",       return_value=0),
        ):
            rc = cli._execute_phases(cfg, [])

        assert "t1" in calls
        assert "t2" in calls
        assert rc == 1

    def test_all_load_jobs_run_regardless_of_sibling_failure(self):
        cfg = _make_cfg(
            load={"l1": _load_job("l1"), "l2": _load_job("l2")},
        )
        calls: List[str] = []
        results = {"l1": 1, "l2": 0}

        def _fake_load(cfg_, name, job, storage):
            calls.append(name)
            return results[name]

        with patch.object(cli, "run_load_job", side_effect=_fake_load):
            rc = cli._execute_phases(cfg, [])

        assert "l1" in calls
        assert "l2" in calls
        assert rc == 1


# ---------------------------------------------------------------------------
# _pass_secret
# ---------------------------------------------------------------------------


class TestPassSecret:
    def test_pointer_ref_goes_directly_into_args(self):
        args, env = [], {}
        cli._pass_secret(args, env, "--secret-ref", "keyring:svc/user", "_ENV_KEY")
        assert args == ["--secret-ref", "keyring:svc/user"]
        assert "_ENV_KEY" not in env

    def test_env_ref_goes_directly_into_args(self):
        args, env = [], {}
        cli._pass_secret(args, env, "--secret-ref", "env:MY_PAT", "_ENV_KEY")
        assert args == ["--secret-ref", "env:MY_PAT"]
        assert "_ENV_KEY" not in env

    def test_bare_literal_injected_into_env_not_args(self):
        args, env = [], {}
        secret = "bare-secret-value"
        cli._pass_secret(args, env, "--secret-ref", secret, "_ENV_KEY")
        # Secret must NOT appear in args
        for arg in args:
            assert secret not in arg
        assert env.get("_ENV_KEY") == secret
        assert args == ["--secret-ref", "env:_ENV_KEY"]


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
            cli._run_component("extractor", ["--help"], cwd="/tmp", env={"_MY_KEY": "myval"})

        assert captured["env"].get("_MY_KEY") == "myval"
        assert "PATH" in captured["env"] or len(captured["env"]) > 1

    def test_no_env_uses_os_environ(self):
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["env"] = kwargs.get("env", {})
            return MagicMock(returncode=0)

        with patch("runner.cli.subprocess.run", side_effect=fake_run):
            cli._run_component("extractor", ["--help"], cwd="/tmp")

        assert len(captured["env"]) > 0


# ---------------------------------------------------------------------------
# Component cwd uses bundled_mappings_root()
# ---------------------------------------------------------------------------


class TestComponentCwd:
    """Components must run with cwd=bundled_mappings_root() so internal relative paths resolve."""

    def _capture_cwd(self, phase_fn, cfg, *args):
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["cwd"] = kwargs.get("cwd")
            return MagicMock(returncode=0)

        with patch("runner.cli.subprocess.run", side_effect=fake_run):
            phase_fn(cfg, *args)

        return captured.get("cwd")

    def test_extraction_uses_bundled_root_as_cwd(self):
        cfg = _make_cfg()
        job = _el_job()
        cwd = self._capture_cwd(cli.run_extraction_job, cfg, "events", job, [])
        assert cwd == bundled_mappings_root()

    def test_transform_uses_bundled_root_as_cwd(self):
        cfg = _make_cfg()
        job = _tx_job()
        cwd = self._capture_cwd(cli.run_transform_job, cfg, "events", job, [])
        assert cwd == bundled_mappings_root()

    def test_load_uses_bundled_root_as_cwd(self):
        cfg = _make_cfg()
        job = _load_job()
        cwd = self._capture_cwd(cli.run_load_job, cfg, "default", job, [])
        assert cwd == bundled_mappings_root()


# ---------------------------------------------------------------------------
# Post-bootstrap plan confirmation
# ---------------------------------------------------------------------------


class TestPlanConfirmGate:
    """After bootstrap, the plan is shown and the user is asked to confirm."""

    def _run_main_with_bootstrap(self, tmp_path, proceed_answer, monkeypatch):
        """
        Simulate tabcloud-run main() where bootstrap fires (no run.yml) and the
        user either confirms or declines the plan.
        """
        config_path = str(tmp_path / "run.yml")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["tabcloud-run", "-c", config_path, "--skip-preflight"])

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            # maybe_bootstrap is imported locally inside main(); patch at source
            patch("runner.bootstrap.maybe_bootstrap", return_value=config_path),
            # load_run_config is also imported locally; patch at source
            patch(
                "runner.config.load_run_config",
                return_value=_make_cfg(
                    extraction={"e": _el_job()},
                    transform={"t": _tx_job()},
                    load={"l": _load_job()},
                ),
            ),
            # validate: always passes
            patch("runner.cli.validate", return_value=MagicMock(ok=True, warnings=[], errors=[])),
            # print_plan: capture call without output
            patch("runner.cli.print_plan") as mock_print_plan,
            # The plan-confirm input
            patch("builtins.input", return_value=proceed_answer),
            # Don't actually run phases
            patch("runner.cli._execute_phases", return_value=0),
        ):
            rc = cli.main()

        return rc, mock_print_plan

    def test_plan_shown_and_proceed_yes_executes(self, tmp_path, monkeypatch):
        rc, mock_plan = self._run_main_with_bootstrap(tmp_path, "y", monkeypatch)
        mock_plan.assert_called_once()
        assert rc == 0

    def test_plan_shown_and_proceed_default_enter_executes(self, tmp_path, monkeypatch):
        rc, mock_plan = self._run_main_with_bootstrap(tmp_path, "", monkeypatch)
        mock_plan.assert_called_once()
        assert rc == 0

    def test_plan_shown_and_decline_exits_0(self, tmp_path, monkeypatch):
        rc, mock_plan = self._run_main_with_bootstrap(tmp_path, "n", monkeypatch)
        mock_plan.assert_called_once()
        assert rc == 0

    def test_no_plan_prompt_when_config_already_exists(self, tmp_path, monkeypatch):
        """When run.yml already exists, skip the plan-confirm gate."""
        config_path = str(tmp_path / "run.yml")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, "argv", ["tabcloud-run", "-c", config_path, "--skip-preflight"])

        # load_run_config is imported locally in main(); patch at source so both
        # call sites see the mock.
        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch(
                "runner.config.load_run_config",
                return_value=_make_cfg(
                    extraction={"e": _el_job()},
                    transform={"t": _tx_job()},
                    load={"l": _load_job()},
                ),
            ),
            # Pretend the file already exists so needs_bootstrap=False
            patch("os.path.isfile", return_value=True),
            patch("runner.cli.validate", return_value=MagicMock(ok=True, warnings=[], errors=[])),
            patch("runner.cli.print_plan") as mock_print_plan,
            patch("builtins.input", return_value=""),
            patch("runner.cli._execute_phases", return_value=0),
        ):
            rc = cli.main()

        # print_plan should NOT have been called (no bootstrap happened)
        mock_print_plan.assert_not_called()
