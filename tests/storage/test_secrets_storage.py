"""Tests for storage/config.py secret providers: static and aws_secrets_manager delegation."""

from __future__ import annotations

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest


def _make_boto3_client(secret_string: str) -> MagicMock:
    mock_boto3 = MagicMock()
    client = mock_boto3.client.return_value
    client.get_secret_value.return_value = {"SecretString": secret_string}
    return mock_boto3


# ---------------------------------------------------------------------------
# static provider
# ---------------------------------------------------------------------------

class TestStaticProvider:
    def _call(self, creds: dict) -> dict:
        from storage.config import _resolve_static_credentials
        return _resolve_static_credentials(creds)

    def test_plain_literals(self):
        creds = {"provider": "static", "access_key": "plain:AKID", "secret_key": "plain:SK"}
        result = self._call(creds)
        assert result == {"key": "AKID", "secret": "SK"}

    def test_bare_literals(self):
        creds = {"provider": "static", "access_key": "AKID_BARE", "secret_key": "SK_BARE"}
        result = self._call(creds)
        assert result == {"key": "AKID_BARE", "secret": "SK_BARE"}

    def test_env_references(self):
        with patch.dict(os.environ, {"MY_AK": "akid-from-env", "MY_SK": "sk-from-env"}):
            creds = {"access_key": "env:MY_AK", "secret_key": "env:MY_SK"}
            result = self._call(creds)
        assert result == {"key": "akid-from-env", "secret": "sk-from-env"}

    def test_keyring_references(self):
        mock_keyring = MagicMock()
        mock_keyring.get_password.side_effect = lambda svc, user: {
            ("tabcloud", "aws-access-key"): "keyring-akid",
            ("tabcloud", "aws-secret-key"): "keyring-sk",
        }[(svc, user)]
        with patch.dict(sys.modules, {"keyring": mock_keyring}):
            creds = {
                "access_key": "keyring:tabcloud/aws-access-key",
                "secret_key": "keyring:tabcloud/aws-secret-key",
            }
            result = self._call(creds)
        assert result == {"key": "keyring-akid", "secret": "keyring-sk"}

    def test_missing_access_key_raises(self):
        from storage.config import _resolve_static_credentials
        with pytest.raises(ValueError, match="access_key"):
            _resolve_static_credentials({"secret_key": "plain:sk"})

    def test_missing_secret_key_raises(self):
        from storage.config import _resolve_static_credentials
        with pytest.raises(ValueError, match="secret_key"):
            _resolve_static_credentials({"access_key": "plain:ak"})

    def test_unresolvable_reference_raises(self):
        """An unresolvable env: reference should raise a descriptive ValueError."""
        import os
        env = {k: v for k, v in os.environ.items() if k != "_UNSET_KEY"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="access_key"):
                self._call({"access_key": "env:_UNSET_KEY", "secret_key": "plain:sk"})


# ---------------------------------------------------------------------------
# aws_secrets_manager delegation
# ---------------------------------------------------------------------------

class TestAwsSecretsManagerProvider:
    def _call(self, sm_cfg: dict) -> dict:
        from storage.config import _resolve_secrets_manager
        return _resolve_secrets_manager(sm_cfg)

    def test_default_key_map(self):
        secret = {"AWS_ACCESS_KEY_ID": "akid-val", "AWS_SECRET_ACCESS_KEY": "sk-val"}
        mock_boto3 = _make_boto3_client(json.dumps(secret))
        with patch.dict(sys.modules, {"boto3": mock_boto3, "json": json}):
            result = self._call({"secret_id": "my/secret"})
        assert result == {"key": "akid-val", "secret": "sk-val"}

    def test_custom_key_map(self):
        secret = {"MY_AK": "custom-akid", "MY_SK": "custom-sk"}
        mock_boto3 = _make_boto3_client(json.dumps(secret))
        with patch.dict(sys.modules, {"boto3": mock_boto3, "json": json}):
            result = self._call({
                "secret_id": "my/secret",
                "key_map": {"access_key": "MY_AK", "secret_key": "MY_SK"},
            })
        assert result == {"key": "custom-akid", "secret": "custom-sk"}

    def test_missing_secret_id_raises(self):
        from storage.config import _resolve_secrets_manager
        with pytest.raises(ValueError, match="secret_id"):
            _resolve_secrets_manager({})

    def test_boto3_error_raises(self):
        mock_boto3 = MagicMock()
        mock_boto3.client.return_value.get_secret_value.side_effect = Exception("no perms")
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            with pytest.raises(ValueError, match="no perms"):
                self._call({"secret_id": "my/secret"})


# ---------------------------------------------------------------------------
# _resolve_credentials dispatch
# ---------------------------------------------------------------------------

class TestResolveCredentials:
    def _call(self, creds: dict) -> dict:
        from storage.config import _resolve_credentials
        return _resolve_credentials(creds)

    def test_aws_default_chain(self):
        assert self._call({"provider": "aws_default_chain"}) == {}

    def test_missing_provider_defaults_to_default_chain(self):
        assert self._call({}) == {}

    def test_aws_profile(self):
        result = self._call({"provider": "aws_profile", "profile": "my-profile"})
        assert result == {"profile": "my-profile"}

    def test_aws_profile_missing_raises(self):
        with pytest.raises(ValueError, match="profile"):
            self._call({"provider": "aws_profile"})

    def test_static_provider_dispatches(self):
        result = self._call({
            "provider": "static",
            "access_key": "plain:AKID",
            "secret_key": "plain:SKID",
        })
        assert result == {"key": "AKID", "secret": "SKID"}

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown credential provider"):
            self._call({"provider": "nonexistent"})
