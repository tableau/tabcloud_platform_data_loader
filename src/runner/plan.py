"""
--explain mode: print the fully-resolved pipeline plan without executing.

Offline only (no network calls). Shows:
  • Resolved storage path and backend.
  • Phase 1 — Extraction: each job, data type, scope/sites, time window or
    lookback, and tuning overrides (if any).
  • Phase 2 — Transform: each transformer, resolved mapping file path and
    existence.
  • Phase 3 — Load: each loader, resolved mapping file path, destination or
    "local only", and secret refs with resolvability.
  • All secret refs (connection + destinations) with a ✓/✗ resolvability
    indicator based on format check and target existence (no signing in).
"""

from __future__ import annotations

import os
import sys
from typing import TextIO

try:
    from common.secrets import redact_ref as _redact_ref
except ImportError:
    def _redact_ref(ref: str) -> str:  # type: ignore[override]
        import re
        if not ref:
            return "(empty)"
        if re.match(r"^(keyring:|env:|file:|awssm:|plain:)", ref):
            return ref
        visible = 2
        if len(ref) <= visible * 2:
            return "*" * len(ref)
        return f"{ref[:visible]}***{ref[-visible:]}"

from runner.config import (
    RunConfig,
    ExtractionJobConfig,
    TransformJobConfig,
    LoadJobConfig,
    DestinationConfig,
    DATA_EVENT_LOGS,
    DATA_SNAPSHOTS,
    DATA_REFERENCE,
    BACKEND_S3,
    resolve_window,
    sites_as_arg,
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def print_plan(cfg: RunConfig, out: TextIO = sys.stdout) -> None:
    """Print the resolved pipeline plan to *out*."""
    _hr(out)
    _line(out, "tabcloud-run  --explain")
    _hr(out)
    _line(out, f"Config dir : {cfg.base_dir}")
    _line(out, f"Storage    : [{cfg.storage.backend}]  {cfg.storage.path}")
    if cfg.storage.backend == BACKEND_S3:
        _line(out, f"             s3://{cfg.storage.s3.bucket}/{cfg.storage.s3.prefix}")
    log_dir = cfg.logging_cfg.dir or os.path.join(cfg.storage.path, "logs")
    _line(out, f"Logs       : {log_dir}  (retention {cfg.logging_cfg.retention_days}d)")
    _line(out)

    # Secrets summary
    _section(out, "Credentials")
    _secret_row(out, "connection.token_secret", cfg.connection.token_secret)
    for name, dest in cfg.destinations.items():
        _secret_row(out, f"destinations.{name}.token_secret", dest.token_secret)
    _line(out)

    # Phase 1 — Extraction
    _section(out, f"Phase 1 — Extraction  ({len(cfg.extraction)} job(s))")
    if cfg.extraction_defaulted:
        _line(out, "  (default jobs — extraction: block was absent)")
    for i, (name, job) in enumerate(cfg.extraction.items(), start=1):
        _line(out, f"  [{i}] {name}  (data: {job.data})")
        _extraction_detail(out, job)
    _line(out)

    # Phase 2 — Transform
    _section(out, f"Phase 2 — Transform  ({len(cfg.transform)} job(s))")
    if cfg.transform_defaulted:
        _line(out, "  (default jobs — transform: block was absent)")
    for i, (name, job) in enumerate(cfg.transform.items(), start=1):
        exists = "✓" if os.path.isfile(job.mapping) else "✗ NOT FOUND"
        _line(out, f"  [{i}] {name}")
        _line(out, f"       mapping : {job.mapping}  [{exists}]")
    _line(out)

    # Phase 3 — Load
    _section(out, f"Phase 3 — Load  ({len(cfg.load)} job(s))")
    if cfg.load_defaulted:
        _line(out, "  (default job — load: block was absent)")
    for i, (name, job) in enumerate(cfg.load.items(), start=1):
        exists = "✓" if os.path.isfile(job.mapping) else "✗ NOT FOUND"
        _line(out, f"  [{i}] {name}")
        _line(out, f"       mapping     : {job.mapping}  [{exists}]")
        if job.destination:
            dest = cfg.destinations.get(job.destination)
            if dest:
                _line(out, f"       destination : {job.destination}  → {dest.server_url}")
                _line(out, f"       datasource  : {job.datasource_name or '(derived from mapping)'}")
                _line(out, f"       mode        : {dest.mode}")
            else:
                _line(out, f"       destination : {job.destination}  [✗ NOT IN destinations]")
        else:
            _line(out, "       destination : (local .hyper only — no publish)")
    _line(out)

    _hr(out)
    _line(out, "No data will be processed.  Run without --explain to execute.")
    _hr(out)


# ---------------------------------------------------------------------------
# Detail helpers
# ---------------------------------------------------------------------------


def _extraction_detail(out: TextIO, job: ExtractionJobConfig) -> None:
    if job.data == DATA_EVENT_LOGS:
        start, end = resolve_window(job)
        _line(out, f"       scope   : {job.scope}")
        _line(out, f"       sites   : {sites_as_arg(job)}")
        if job.start_time and job.end_time:
            _line(out, f"       window  : {job.start_time} → {job.end_time}  (fixed)")
        else:
            _line(out, f"       window  : {start} → {end}  (lookback {job.lookback_days}d)")
        if job.event_types:
            _line(out, f"       types   : {', '.join(job.event_types)}")
    elif job.data == DATA_SNAPSHOTS:
        _line(out, f"       sites         : {sites_as_arg(job)}")
        _line(out, f"       cadence       : {job.cadence}  ({job.lookback_intervals} intervals)")
        _line(out, f"       selection     : {job.selection}")
        _line(out, f"       gap_action    : {job.gap_action}")
        if job.entity_types:
            _line(out, f"       entity_types  : {', '.join(job.entity_types)}")
    elif job.data == DATA_REFERENCE:
        _line(out, f"       entities : {', '.join(job.entities) or '(defaults)'}")


def _secret_row(out: TextIO, label: str, ref: str) -> None:
    ok = _check_ref(ref)
    symbol = "✓" if ok else "✗"
    _line(out, f"  {symbol}  {label}: {_redact_ref(ref) or '(empty)'}")


def _check_ref(ref: str) -> bool:
    """Offline resolvability check: format check + file/env existence where applicable."""
    if not ref:
        return False
    import re
    if re.match(r"^keyring:", ref):
        try:
            import keyring
            parts = ref[8:]  # strip "keyring:"
            if "/" in parts:
                service, username = parts.split("/", 1)
            else:
                return True  # can't check without service/username
            val = keyring.get_password(service, username)
            return val is not None
        except Exception:
            return True  # keyring may not be installed; give benefit of doubt
    if re.match(r"^env:", ref):
        var = ref[4:]
        return bool(os.environ.get(var))
    if re.match(r"^file:", ref):
        path = ref[5:]
        return os.path.isfile(path)
    if re.match(r"^(awssm:|plain:)", ref):
        return True  # can't verify without network; assume ok
    # Bare literal — present but not a secret_ref
    return bool(ref)


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _line(out: TextIO, text: str = "") -> None:
    print(text, file=out)


def _hr(out: TextIO) -> None:
    print("─" * 72, file=out)


def _section(out: TextIO, title: str) -> None:
    print(f"\n  {title}", file=out)
    print(f"  {'─' * (len(title) + 2)}", file=out)
