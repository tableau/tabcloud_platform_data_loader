"""
Unified secret-resolution layer.

A secret value anywhere in this project's config may be either:
  - A bare literal string (back-compat; treated as ``plain:``).
  - A ``scheme:locator`` reference resolved at runtime by a registered provider.

Built-in schemes
----------------
  plain:LITERAL             The literal string after the colon (no lookup).
  env:VAR_NAME              Read the named environment variable.
  file:/path/to/secret      Read the file; strip one trailing newline.
  keyring:SERVICE/USERNAME  Retrieve from the OS keychain (requires ``keyring``).
  awssm:SECRET_ID           Retrieve a plaintext secret from AWS Secrets Manager.
  awssm:SECRET_ID#JSON_KEY  Retrieve a JSON-encoded secret and extract one field.

Disambiguation rule
-------------------
A string is treated as a reference only when the substring before the first ``:``
exactly matches a registered scheme name AND contains no whitespace or colon.
Everything else is a literal (``plain``).  This means existing PATs or tokens that
happen to contain ``:`` are never misidentified.

Extensibility
-------------
Third-party providers (HashiCorp Vault, GCP Secret Manager, Azure Key Vault, …)
can register themselves with :func:`register_provider` before any call to
:func:`resolve_secret`.

  from common.secrets import register_provider

  def _my_vault_provider(locator: str) -> str:
      ...

  register_provider("vault", _my_vault_provider)

  # Config can then use:  token_secret = vault:secret/data/tabcloud#pat
"""

from __future__ import annotations

import os
from typing import Callable, Dict, Optional, Tuple

# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class SecretResolutionError(Exception):
    """Raised when a secret reference cannot be resolved.

    The message is always safe to log — it describes the failure without
    including the secret value.
    """


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------

_REGISTRY: Dict[str, Callable[[str], str]] = {}


def register_provider(scheme: str, fn: Callable[[str], str]) -> None:
    """Register a provider function for *scheme*.

    *fn* receives the *locator* portion of a ``scheme:locator`` reference and
    must return the resolved plaintext secret or raise
    :class:`SecretResolutionError`.

    Registering a scheme that already exists overwrites the previous provider.
    """
    _REGISTRY[scheme.lower()] = fn


# ---------------------------------------------------------------------------
# Reference parsing
# ---------------------------------------------------------------------------

def is_reference(value: str) -> bool:
    """Return True if *value* looks like a ``scheme:locator`` reference.

    A string is a reference only when the part before the first ``:`` is a
    non-empty, whitespace-free string that exactly matches a registered scheme.
    """
    if not value or ":" not in value:
        return False
    prefix = value[: value.index(":")]
    return bool(prefix) and " " not in prefix and prefix.lower() in _REGISTRY


def parse_reference(value: str) -> Tuple[str, str]:
    """Parse a ``scheme:locator`` reference into ``(scheme, locator)``.

    Applies the disambiguation rule: if the value is not a recognised
    reference, returns ``("plain", value)``.
    """
    if not value or ":" not in value:
        return "plain", value
    idx = value.index(":")
    prefix = value[:idx].lower()
    if prefix and " " not in prefix and prefix in _REGISTRY:
        return prefix, value[idx + 1 :]
    return "plain", value


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------

def resolve_secret(value: str) -> str:
    """Resolve *value* to a plaintext secret.

    Accepts either a bare literal (returned as-is) or a ``scheme:locator``
    reference dispatched to the appropriate provider.

    Raises :class:`SecretResolutionError` if resolution fails.  The error
    message is safe to log.
    """
    if not value:
        return value
    scheme, locator = parse_reference(value)
    provider = _REGISTRY.get(scheme)
    if provider is None:
        raise SecretResolutionError(
            f"Unknown secret scheme {scheme!r}. "
            f"Registered schemes: {sorted(_REGISTRY)}."
        )
    return provider(locator)


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def redact(value: str, visible: int = 2) -> str:
    """Return a redacted representation safe for log messages.

    Shows the first and last *visible* characters of the resolved value with
    ``***`` in the middle so you can correlate entries without exposing the
    secret, e.g. ``"ab***yz"`` or ``"ke***ng"`` for ``"keyring"``.
    """
    if not value:
        return "(empty)"
    if len(value) <= visible * 2:
        return "*" * len(value)
    return f"{value[:visible]}***{value[-visible:]}"


def redact_ref(ref: str) -> str:
    """Return a log-safe description of a secret *reference* (not the value).

    For pointer references (env/keyring/file/awssm) the reference itself is
    not sensitive, so it is returned as-is.  For a ``plain:`` or bare literal
    the value is redacted.
    """
    if not ref:
        return "(empty)"
    scheme, locator = parse_reference(ref)
    if scheme == "plain":
        return redact(locator)
    return ref  # e.g. "keyring:tabcloud/extractor-pat" — safe to log


# ---------------------------------------------------------------------------
# Built-in providers
# ---------------------------------------------------------------------------

# --- plain ---

def _provide_plain(locator: str) -> str:
    return locator


register_provider("plain", _provide_plain)


# --- env ---

def _provide_env(locator: str) -> str:
    value = os.environ.get(locator)
    if value is None:
        raise SecretResolutionError(
            f"Environment variable {locator!r} is not set. "
            f"Export it before running: export {locator}=<your-secret>"
        )
    if not value:
        raise SecretResolutionError(
            f"Environment variable {locator!r} is set but empty."
        )
    return value


register_provider("env", _provide_env)


# --- file ---

def _provide_file(locator: str) -> str:
    path = locator
    if not os.path.isabs(path):
        # Relative paths are resolved from cwd at call time.
        path = os.path.abspath(path)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().rstrip("\n")
    except FileNotFoundError:
        raise SecretResolutionError(
            f"Secret file not found: {path!r}. "
            f"Create it (chmod 600 recommended) and write the secret as its only content."
        )
    except PermissionError:
        raise SecretResolutionError(
            f"Permission denied reading secret file: {path!r}."
        )
    except OSError as exc:
        raise SecretResolutionError(
            f"Could not read secret file {path!r}: {exc}"
        )


register_provider("file", _provide_file)


# --- keyring ---

_KEYRING_INSTALL_HINT = (
    "The 'keyring' package is required for keyring: references.\n"
    "Install it with:  pip install -e .[secrets]\n"
    "Then store your secret:  tabcloud-secrets set <service> <username>\n"
    "Run 'tabcloud-secrets doctor' to verify your keyring backend is working."
)


def _provide_keyring(locator: str) -> str:
    """Retrieve a secret from the OS keychain.

    *locator* format: ``SERVICE/USERNAME`` (both parts required).
    """
    try:
        import keyring as _keyring
    except ImportError:
        raise SecretResolutionError(
            f"Cannot resolve keyring:{locator} — keyring library not installed.\n"
            + _KEYRING_INSTALL_HINT
        )

    if "/" not in locator:
        raise SecretResolutionError(
            f"keyring: references must be in the form keyring:SERVICE/USERNAME, "
            f"got keyring:{locator!r}. "
            f"Example: keyring:tabcloud/extractor-pat"
        )
    service, username = locator.split("/", 1)

    try:
        value = _keyring.get_password(service, username)
    except Exception as exc:
        raise SecretResolutionError(
            f"Keyring lookup failed for service={service!r} username={username!r}: {exc}\n"
            f"Run 'tabcloud-secrets doctor' to check your keyring backend.\n"
            f"Re-store the secret: tabcloud-secrets set {service} {username}"
        )

    if value is None:
        raise SecretResolutionError(
            f"No secret found in keyring for service={service!r} username={username!r}.\n"
            f"Store it first: tabcloud-secrets set {service} {username}\n"
            f"Run 'tabcloud-secrets doctor' to verify the backend is accessible."
        )
    return value


register_provider("keyring", _provide_keyring)


# --- awssm (AWS Secrets Manager) ---

_AWSSM_INSTALL_HINT = (
    "boto3 is required for awssm: references.\n"
    "Install it with:  pip install -e .[s3]\n"
    "(boto3 is bundled with the S3 storage extra.)"
)


def _provide_awssm(locator: str) -> str:
    """Retrieve a secret from AWS Secrets Manager.

    *locator* formats:
      ``SECRET_ID``           — the full secret string value.
      ``SECRET_ID#JSON_KEY``  — parse the secret as JSON, extract *JSON_KEY*.
    """
    try:
        import boto3  # type: ignore[import-untyped]
        import json as _json
    except ImportError:
        raise SecretResolutionError(
            f"Cannot resolve awssm:{locator} — boto3 not installed.\n"
            + _AWSSM_INSTALL_HINT
        )

    if "#" in locator:
        secret_id, json_key = locator.split("#", 1)
    else:
        secret_id, json_key = locator, None

    try:
        client = boto3.client("secretsmanager")
        resp = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        raise SecretResolutionError(
            f"AWS Secrets Manager lookup failed for secret {secret_id!r}: {exc}\n"
            f"Check your AWS credentials and that the secret exists in the expected region."
        )

    raw = resp.get("SecretString")
    if raw is None:
        raise SecretResolutionError(
            f"AWS Secrets Manager secret {secret_id!r} is binary — only string secrets are supported."
        )

    if json_key is None:
        return raw

    try:
        data = _json.loads(raw)
    except Exception:
        raise SecretResolutionError(
            f"AWS Secrets Manager secret {secret_id!r} is not valid JSON "
            f"but a JSON key ({json_key!r}) was requested."
        )

    if json_key not in data:
        raise SecretResolutionError(
            f"JSON key {json_key!r} not found in AWS Secrets Manager secret {secret_id!r}. "
            f"Available keys: {sorted(data.keys())}"
        )

    return str(data[json_key])


register_provider("awssm", _provide_awssm)
