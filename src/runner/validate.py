"""
Static pre-flight validation for RunConfig.

All checks are purely offline (no network, no file reads beyond existence).
Two severity levels:
  • errors   — abort; runner exits non-zero before touching any data.
  • warnings — surfaced to the user; run proceeds.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from runner.config import (
    RunConfig,
    VALID_DATA_TYPES,
    VALID_BACKENDS,
    VALID_DEST_MODES,
    BACKEND_S3,
    DATA_EVENT_LOGS,
    DATA_SNAPSHOTS,
    DATA_REFERENCE,
    bundled_mappings_root,
)

try:
    from common.secrets import is_reference as _is_reference, redact as _redact
except ImportError:
    def _is_reference(value: str) -> bool:  # type: ignore[override]
        return bool(
            re.match(r"^(keyring:|env:|file:|awssm:|plain:)", value or "")
        )

    def _redact(value: str, visible: int = 2) -> str:  # type: ignore[override]
        if not value:
            return "(empty)"
        if len(value) <= visible * 2:
            return "*" * len(value)
        return f"{value[:visible]}***{value[-visible:]}"


@dataclass
class ValidationResult:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)


_URL_RE = re.compile(r"^https://[^/#?]+$", re.IGNORECASE)


def validate(cfg: RunConfig) -> ValidationResult:
    """
    Run all static checks against cfg. Returns a ValidationResult.
    Caller should check result.ok; if False, abort before any execution.
    """
    result = ValidationResult()
    _check_connection(cfg, result)
    _check_extraction(cfg, result)
    _check_transform(cfg, result)
    _check_load(cfg, result)
    _check_destinations(cfg, result)
    _check_storage(cfg, result)
    _check_output_uniqueness(cfg, result)
    return result


# ---------------------------------------------------------------------------
# Section checkers
# ---------------------------------------------------------------------------


def _check_connection(cfg: RunConfig, r: ValidationResult) -> None:
    conn = cfg.connection
    if not conn.tcm_url:
        r.add_error("connection.tcm_url is required.")
    elif not _URL_RE.match(conn.tcm_url):
        r.add_error(
            f"connection.tcm_url does not look like a base HTTPS URL (no path, no fragment): "
            f"'{conn.tcm_url}'. Example: https://your-tenant.online.tableau.com"
        )
    if not conn.token_name:
        r.add_error("connection.token_name is required.")
    if not conn.token_secret:
        r.add_error("connection.token_secret is required.")
    else:
        _check_secret_ref("connection.token_secret", conn.token_secret, r, warn_only=False)


def _check_extraction(cfg: RunConfig, r: ValidationResult) -> None:
    if not cfg.extraction:
        r.add_warning("No extraction jobs are defined. Nothing will be extracted.")
        return
    for name, job in cfg.extraction.items():
        prefix = f"extraction.{name}"
        if not job.data:
            r.add_error(f"{prefix}: `data` is required (one of: {sorted(VALID_DATA_TYPES)}).")
        elif job.data not in VALID_DATA_TYPES:
            r.add_error(
                f"{prefix}.data: '{job.data}' is not valid. "
                f"Must be one of: {sorted(VALID_DATA_TYPES)}."
            )
        if job.data == DATA_EVENT_LOGS:
            if job.scope not in ("site", "tenant"):
                r.add_error(f"{prefix}.scope: must be 'site' or 'tenant'; got '{job.scope}'.")
        if job.data == DATA_SNAPSHOTS:
            if job.selection not in ("latest", "all"):
                r.add_error(f"{prefix}.selection: must be 'latest' or 'all'; got '{job.selection}'.")
            if job.cadence not in ("hourly", "daily"):
                r.add_error(f"{prefix}.cadence: must be 'hourly' or 'daily'; got '{job.cadence}'.")
            if job.gap_action not in ("warn", "error", "ignore"):
                r.add_error(f"{prefix}.gap_action: must be 'warn', 'error', or 'ignore'.")
        if job.start_time and not _is_iso8601(job.start_time):
            r.add_error(f"{prefix}.start_time: not a valid ISO 8601 timestamp: '{job.start_time}'.")
        if job.end_time and not _is_iso8601(job.end_time):
            r.add_error(f"{prefix}.end_time: not a valid ISO 8601 timestamp: '{job.end_time}'.")
        if bool(job.start_time) != bool(job.end_time):
            r.add_error(f"{prefix}: start_time and end_time must both be set or both be absent.")


def _check_transform(cfg: RunConfig, r: ValidationResult) -> None:
    if not cfg.transform:
        r.add_warning("No transform jobs defined. Nothing will be transformed.")
        return
    for name, job in cfg.transform.items():
        prefix = f"transform.{name}"
        if not job.mapping:
            r.add_error(f"{prefix}.mapping: path is required.")
        elif not os.path.isfile(job.mapping):
            if cfg.transform_defaulted:
                r.add_error(
                    f"{prefix}.mapping: bundled default mapping not found: {job.mapping}\n"
                    f"  These are the standard transform mappings shipped with "
                    f"tabcloud-platform-data-loader.  The runner looks for them "
                    f"under the package's mappings/ directory:\n"
                    f"    {bundled_mappings_root()}/mappings/\n"
                    f"  To fix this, either:\n"
                    f"    1. Run tabcloud-run from the source checkout directory "
                    f"(where mappings/ exists), or\n"
                    f"    2. Add an explicit transform: block to run.yml pointing "
                    f"to your own mapping files."
                )
            else:
                r.add_error(f"{prefix}.mapping: file not found: {job.mapping}")


def _check_load(cfg: RunConfig, r: ValidationResult) -> None:
    if not cfg.load:
        r.add_warning("No load jobs defined. Nothing will be loaded.")
        return
    for name, job in cfg.load.items():
        prefix = f"load.{name}"
        if not job.mapping:
            r.add_error(f"{prefix}.mapping: path is required.")
        elif not os.path.isfile(job.mapping):
            if cfg.load_defaulted:
                r.add_error(
                    f"{prefix}.mapping: bundled default mapping not found: {job.mapping}\n"
                    f"  This is the standard loader mapping shipped with "
                    f"tabcloud-platform-data-loader.  The runner looks for it "
                    f"under the package's mappings/ directory:\n"
                    f"    {bundled_mappings_root()}/mappings/\n"
                    f"  To fix this, either:\n"
                    f"    1. Run tabcloud-run from the source checkout directory "
                    f"(where mappings/ exists), or\n"
                    f"    2. Add an explicit load: block to run.yml pointing "
                    f"to your own mapping file."
                )
            else:
                r.add_error(f"{prefix}.mapping: file not found: {job.mapping}")
        if job.destination is not None:
            if job.destination not in cfg.destinations:
                r.add_error(
                    f"{prefix}.destination: '{job.destination}' is not defined in "
                    f"destinations. Available: {sorted(cfg.destinations.keys()) or '(none)'}."
                )
            if not job.datasource_name:
                r.add_warning(
                    f"{prefix}: destination '{job.destination}' is set but no `name` "
                    f"(published data source name) is specified. "
                    f"The loader will derive a name from the mapping file."
                )


def _check_destinations(cfg: RunConfig, r: ValidationResult) -> None:
    for name, dest in cfg.destinations.items():
        prefix = f"destinations.{name}"
        if dest.type != "tableau":
            r.add_error(
                f"{prefix}.type: '{dest.type}' is not supported. "
                f"Only 'tableau' is supported today."
            )
        if not dest.server_url:
            r.add_error(f"{prefix}.server_url: required.")
        elif not _URL_RE.match(dest.server_url):
            r.add_error(
                f"{prefix}.server_url does not look like a base HTTPS URL: "
                f"'{dest.server_url}'. Example: https://your-pod.online.tableau.com"
            )
        if not dest.token_name:
            r.add_error(f"{prefix}.token_name: required.")
        if not dest.token_secret:
            r.add_error(f"{prefix}.token_secret: required.")
        else:
            _check_secret_ref(f"{prefix}.token_secret", dest.token_secret, r, warn_only=False)
        if dest.mode not in VALID_DEST_MODES:
            r.add_error(
                f"{prefix}.mode: '{dest.mode}' is not valid. "
                f"Must be one of: {sorted(VALID_DEST_MODES)}."
            )


def _check_storage(cfg: RunConfig, r: ValidationResult) -> None:
    st = cfg.storage
    if st.backend not in VALID_BACKENDS:
        r.add_error(
            f"storage.backend: '{st.backend}' is not valid. "
            f"Must be one of: {sorted(VALID_BACKENDS)}."
        )
    if not st.path:
        r.add_error("storage.path: required.")
    if st.backend == BACKEND_S3:
        if not st.s3.bucket:
            r.add_error("storage.s3.bucket: required when backend is 's3'.")
        creds = st.s3.credentials
        provider = creds.provider
        if provider not in (
            "aws_default_chain", "static", "aws_profile", "aws_secrets_manager"
        ):
            r.add_error(
                f"storage.s3.credentials.provider: '{provider}' is not valid. "
                f"Choose: aws_default_chain | static | aws_profile | aws_secrets_manager."
            )
        if provider == "static":
            if not creds.access_key:
                r.add_error("storage.s3.credentials.access_key: required for 'static' provider.")
            else:
                _check_secret_ref(
                    "storage.s3.credentials.access_key", creds.access_key, r, warn_only=True
                )
            if not creds.secret_key:
                r.add_error("storage.s3.credentials.secret_key: required for 'static' provider.")
            else:
                _check_secret_ref(
                    "storage.s3.credentials.secret_key", creds.secret_key, r, warn_only=True
                )
        if provider == "aws_profile" and not creds.profile:
            r.add_error(
                "storage.s3.credentials.profile: required for 'aws_profile' provider."
            )
        if provider == "aws_secrets_manager" and not creds.secrets_manager_id:
            r.add_error(
                "storage.s3.credentials.secrets_manager.secret_id: "
                "required for 'aws_secrets_manager' provider."
            )


def _check_output_uniqueness(cfg: RunConfig, r: ValidationResult) -> None:
    """
    Warn if two transformer jobs write to the same output file (transformer mappings
    that share the same target output path).
    Check that each destination+datasource_name pair is used by at most one loader
    to avoid two loaders publishing under the same data source name.
    """
    # Transformer output uniqueness (by mapping path — exact duplicates)
    seen_mappings: Dict[str, str] = {}
    for name, job in cfg.transform.items():
        if job.mapping in seen_mappings:
            r.add_warning(
                f"transform.{name} and transform.{seen_mappings[job.mapping]} "
                f"both reference the same mapping file: {job.mapping}. "
                f"They will produce identical output, which wastes time."
            )
        seen_mappings[job.mapping] = name

    # Load destination uniqueness: (destination, datasource_name) must be unique.
    seen_publish: Dict[Tuple[str, str], str] = {}
    for name, job in cfg.load.items():
        if job.destination is None:
            continue
        key = (job.destination, job.datasource_name or "")
        if key in seen_publish:
            r.add_error(
                f"load.{name} and load.{seen_publish[key]} both publish to "
                f"destination '{job.destination}' with the same data source name "
                f"'{job.datasource_name}'. Use distinct datasource names."
            )
        seen_publish[key] = name


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_secret_ref(field_name: str, value: str, r: ValidationResult, *, warn_only: bool) -> None:
    """
    Emit a warning when a field that should hold a secret_ref looks like a bare
    plaintext value. The value itself is never included in the message.
    """
    if not value:
        return
    if _is_reference(value):
        return  # looks like a proper ref
    msg = (
        f"{field_name}: {_redact(value)!r} looks like a plaintext secret. "
        f"Use a secret_ref instead: keyring:SERVICE/KEY, env:VAR_NAME, "
        f"file:/path/to/file, or awssm:SECRET_ID#KEY."
    )
    r.add_warning(msg)  # always warn (never block; user may have a reason)


_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"
)


def _is_iso8601(value: str) -> bool:
    return bool(_ISO8601_RE.match(value.strip()))
