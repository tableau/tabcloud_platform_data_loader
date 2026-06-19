"""Tests for common.secrets — provider registry, parsing, resolution, redaction."""

from __future__ import annotations

import os
import sys
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from common.secrets import (
    SecretResolutionError,
    is_reference,
    parse_reference,
    redact,
    redact_ref,
    register_provider,
    resolve_secret,
)


# ---------------------------------------------------------------------------
# parse_reference + is_reference
# ---------------------------------------------------------------------------

class TestParseReference:
    def test_plain_bare_literal(self):
        assert parse_reference("my-secret-value") == ("plain", "my-secret-value")

    def test_plain_explicit(self):
        assert parse_reference("plain:my-secret") == ("plain", "my-secret")

    def test_env_reference(self):
        assert parse_reference("env:MY_VAR") == ("env", "MY_VAR")

    def test_file_reference(self):
        assert parse_reference("file:/tmp/secret.txt") == ("file", "/tmp/secret.txt")

    def test_keyring_reference(self):
        assert parse_reference("keyring:tabcloud/extractor-pat") == ("keyring", "tabcloud/extractor-pat")

    def test_awssm_reference(self):
        assert parse_reference("awssm:my/secret#KEY") == ("awssm", "my/secret#KEY")

    def test_unknown_scheme_treated_as_plain(self):
        # An unregistered prefix → treated as literal
        assert parse_reference("ftp:something") == ("plain", "ftp:something")

    def test_jwt_token_with_colon_is_plain(self):
        # A real JWT token has colons (e.g. base64 padding with = but also could have colons
        # in practice); more importantly, a URL-like string that starts with http should not
        # be mis-parsed since "http" is not a registered scheme.
        assert parse_reference("https://example.com/token") == ("plain", "https://example.com/token")

    def test_scheme_with_whitespace_treated_as_plain(self):
        assert parse_reference("has space:locator") == ("plain", "has space:locator")

    def test_empty_string(self):
        assert parse_reference("") == ("plain", "")

    def test_no_colon(self):
        assert parse_reference("noschemeatall") == ("plain", "noschemeatall")

    def test_is_reference_env(self):
        assert is_reference("env:MY_VAR") is True

    def test_is_reference_plain_literal(self):
        assert is_reference("some-token") is False

    def test_is_reference_unknown_scheme(self):
        assert is_reference("vault:my/secret") is False

    def test_is_reference_empty(self):
        assert is_reference("") is False


# ---------------------------------------------------------------------------
# plain provider
# ---------------------------------------------------------------------------

class TestPlainProvider:
    def test_plain_locator(self):
        assert resolve_secret("plain:hello") == "hello"

    def test_bare_literal(self):
        assert resolve_secret("a-plain-token-value") == "a-plain-token-value"

    def test_empty_string(self):
        assert resolve_secret("") == ""

    def test_literal_with_colon(self):
        # A string like "token:with:colons" should be treated as plain (plain prefix not registered)
        # because "token" is not a registered scheme.
        assert resolve_secret("token:with:colons") == "token:with:colons"


# ---------------------------------------------------------------------------
# env provider
# ---------------------------------------------------------------------------

class TestEnvProvider:
    def test_resolves_env_var(self):
        with patch.dict(os.environ, {"_TEST_SECRET": "abc123"}):
            assert resolve_secret("env:_TEST_SECRET") == "abc123"

    def test_missing_env_var_raises(self):
        env = {k: v for k, v in os.environ.items() if k != "_MISSING_VAR"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(SecretResolutionError, match="_MISSING_VAR"):
                resolve_secret("env:_MISSING_VAR")

    def test_empty_env_var_raises(self):
        with patch.dict(os.environ, {"_EMPTY_VAR": ""}):
            with pytest.raises(SecretResolutionError, match="_EMPTY_VAR"):
                resolve_secret("env:_EMPTY_VAR")

    def test_error_message_contains_remediation(self):
        env = {k: v for k, v in os.environ.items() if k != "_UNSET"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(SecretResolutionError, match="export"):
                resolve_secret("env:_UNSET")


# ---------------------------------------------------------------------------
# file provider
# ---------------------------------------------------------------------------

class TestFileProvider:
    def test_resolves_file(self, tmp_path):
        secret_file = tmp_path / "secret.txt"
        secret_file.write_text("my-file-secret\n")
        assert resolve_secret(f"file:{secret_file}") == "my-file-secret"

    def test_strips_trailing_newline(self, tmp_path):
        secret_file = tmp_path / "secret.txt"
        secret_file.write_text("value\n")
        assert resolve_secret(f"file:{secret_file}") == "value"

    def test_missing_file_raises(self, tmp_path):
        missing = tmp_path / "does_not_exist.txt"
        with pytest.raises(SecretResolutionError, match="not found"):
            resolve_secret(f"file:{missing}")

    def test_error_message_contains_remediation(self, tmp_path):
        missing = tmp_path / "nope.txt"
        with pytest.raises(SecretResolutionError, match="chmod"):
            resolve_secret(f"file:{missing}")


# ---------------------------------------------------------------------------
# keyring provider
# ---------------------------------------------------------------------------

class TestKeyringProvider:
    def test_resolves_keyring(self):
        mock_keyring = MagicMock()
        mock_keyring.get_password.return_value = "stored-secret"
        with patch.dict(sys.modules, {"keyring": mock_keyring}):
            result = resolve_secret("keyring:tabcloud/extractor-pat")
        assert result == "stored-secret"
        mock_keyring.get_password.assert_called_once_with("tabcloud", "extractor-pat")

    def test_missing_keyring_raises_with_install_hint(self):
        with patch.dict(sys.modules, {"keyring": None}):
            with pytest.raises((SecretResolutionError, ImportError)):
                resolve_secret("keyring:tabcloud/extractor-pat")

    def test_no_password_found_raises(self):
        mock_keyring = MagicMock()
        mock_keyring.get_password.return_value = None
        with patch.dict(sys.modules, {"keyring": mock_keyring}):
            with pytest.raises(SecretResolutionError, match="No secret found"):
                resolve_secret("keyring:tabcloud/extractor-pat")

    def test_no_slash_in_locator_raises(self):
        mock_keyring = MagicMock()
        mock_keyring.get_password.return_value = "x"
        with patch.dict(sys.modules, {"keyring": mock_keyring}):
            with pytest.raises(SecretResolutionError, match="SERVICE/USERNAME"):
                resolve_secret("keyring:no-slash-here")

    def test_error_message_contains_remediation(self):
        mock_keyring = MagicMock()
        mock_keyring.get_password.return_value = None
        with patch.dict(sys.modules, {"keyring": mock_keyring}):
            with pytest.raises(SecretResolutionError, match="tabcloud-secrets"):
                resolve_secret("keyring:tabcloud/pat")


# ---------------------------------------------------------------------------
# awssm provider
# ---------------------------------------------------------------------------

class TestAwssmProvider:
    def _mock_boto3(self, secret_string: str) -> MagicMock:
        mock_boto3 = MagicMock()
        client = mock_boto3.client.return_value
        client.get_secret_value.return_value = {"SecretString": secret_string}
        return mock_boto3

    def test_plain_string_secret(self):
        mock_boto3 = self._mock_boto3("my-plaintext-secret")
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            result = resolve_secret("awssm:my/secret")
        assert result == "my-plaintext-secret"
        mock_boto3.client.return_value.get_secret_value.assert_called_once_with(SecretId="my/secret")

    def test_json_secret_with_key(self):
        import json
        mock_boto3 = self._mock_boto3(json.dumps({"PAT": "json-secret-value", "OTHER": "x"}))
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            result = resolve_secret("awssm:my/secret#PAT")
        assert result == "json-secret-value"

    def test_missing_json_key_raises(self):
        import json
        mock_boto3 = self._mock_boto3(json.dumps({"OTHER": "x"}))
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            with pytest.raises(SecretResolutionError, match="MISSING_KEY"):
                resolve_secret("awssm:my/secret#MISSING_KEY")

    def test_non_json_with_key_raises(self):
        mock_boto3 = self._mock_boto3("not-json")
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            with pytest.raises(SecretResolutionError, match="not valid JSON"):
                resolve_secret("awssm:my/secret#KEY")

    def test_boto3_error_raises(self):
        mock_boto3 = MagicMock()
        mock_boto3.client.return_value.get_secret_value.side_effect = Exception("Access denied")
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            with pytest.raises(SecretResolutionError, match="Access denied"):
                resolve_secret("awssm:my/secret")


# ---------------------------------------------------------------------------
# register_provider + unknown scheme
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_register_custom_provider(self):
        def _custom(locator: str) -> str:
            return f"custom:{locator}"

        register_provider("_testcustom", _custom)
        try:
            result = resolve_secret("_testcustom:mykey")
            assert result == "custom:mykey"
        finally:
            from common import secrets as _sec
            del _sec._REGISTRY["_testcustom"]

    def test_unknown_scheme_treated_as_plain_literal(self):
        # The disambiguation rule: an unregistered "scheme" prefix is treated as a plain
        # literal, not an error.  This preserves back-compat for strings like "http://…"
        # or PAT values that happen to contain a colon.
        result = resolve_secret("nonexistentscheme999:locator")
        assert result == "nonexistentscheme999:locator"

    def test_overwrite_provider(self):
        original = None
        from common.secrets import _REGISTRY
        original = _REGISTRY.get("plain")
        try:
            register_provider("plain", lambda loc: "overridden")
            assert resolve_secret("plain:anything") == "overridden"
        finally:
            if original is not None:
                _REGISTRY["plain"] = original


# ---------------------------------------------------------------------------
# redact helpers
# ---------------------------------------------------------------------------

class TestRedact:
    def test_redacts_long_value(self):
        result = redact("abcdefgh")
        assert "***" in result
        assert result.startswith("ab")
        assert result.endswith("gh")
        assert "abcdefgh" not in result

    def test_redacts_short_value(self):
        result = redact("abc")
        assert result == "***"

    def test_empty_returns_marker(self):
        assert redact("") == "(empty)"

    def test_redact_ref_pointer_returns_ref(self):
        ref = "keyring:tabcloud/extractor-pat"
        assert redact_ref(ref) == ref  # safe to log as-is

    def test_redact_ref_plain_redacts_value(self):
        result = redact_ref("plain:my-secret-value")
        assert "my-secret-value" not in result
        assert "***" in result

    def test_redact_ref_bare_literal_redacts(self):
        result = redact_ref("my-literal-secret")
        assert "my-literal-secret" not in result

    def test_redact_ref_env_is_safe(self):
        ref = "env:MY_VAR"
        assert redact_ref(ref) == ref
