"""Tests for runner.plan — --explain output, including secret redaction."""

from __future__ import annotations

import io
import os

import pytest

from runner.config import (
    RunConfig,
    ConnectionConfig,
    ExtractionJobConfig,
    TransformJobConfig,
    LoadJobConfig,
    StorageConfig,
    LoggingConfig,
    DATA_EVENT_LOGS,
)
from runner.plan import print_plan


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg(token_secret="keyring:tabcloud/pat", **overrides) -> RunConfig:
    kwargs = dict(
        version=1,
        base_dir="/tmp",
        connection=ConnectionConfig(
            tcm_url="https://example.online.tableau.com",
            token_name="pat",
            token_secret=token_secret,
        ),
        extraction={
            "events": ExtractionJobConfig(name="events", data=DATA_EVENT_LOGS),
        },
        transform={},
        load={},
        destinations={},
        storage=StorageConfig(path="/tmp/data"),
        logging_cfg=LoggingConfig(),
    )
    kwargs.update(overrides)
    return RunConfig(**kwargs)


def _plan_output(cfg: RunConfig) -> str:
    buf = io.StringIO()
    print_plan(cfg, out=buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Secret redaction in --explain output
# ---------------------------------------------------------------------------


class TestPlanSecretRedaction:
    def test_keyring_ref_printed_verbatim(self):
        """Non-sensitive keyring: refs must appear in full in the plan output."""
        cfg = _cfg(token_secret="keyring:tabcloud/extractor-pat")
        output = _plan_output(cfg)
        assert "keyring:tabcloud/extractor-pat" in output

    def test_env_ref_printed_verbatim(self):
        cfg = _cfg(token_secret="env:MY_PAT_SECRET")
        output = _plan_output(cfg)
        assert "env:MY_PAT_SECRET" in output

    def test_file_ref_printed_verbatim(self):
        cfg = _cfg(token_secret="file:/run/secrets/pat")
        output = _plan_output(cfg)
        assert "file:/run/secrets/pat" in output

    def test_bare_literal_secret_is_redacted(self):
        """A bare plaintext PAT must NOT appear verbatim in the --explain output."""
        raw_secret = "AB/Sv8v+QdafITmoMK+TjA==:ItYSTAqCdyDO546X_bwRzklnPiDgJ94dqfB5bEPNaIg"
        cfg = _cfg(token_secret=raw_secret)
        output = _plan_output(cfg)
        assert raw_secret not in output, "Bare plaintext secret must be redacted in --explain output"
        # A redacted placeholder must appear instead
        assert "***" in output

    def test_short_bare_literal_is_redacted(self):
        """Very short secrets must also be masked (all stars)."""
        # Use a value that won't accidentally appear in other output (not a substring of 'tabcloud')
        short_secret = "xyzzy"
        cfg = _cfg(token_secret=short_secret)
        output = _plan_output(cfg)
        assert short_secret not in output
        assert "***" in output or "*" * len(short_secret) in output
