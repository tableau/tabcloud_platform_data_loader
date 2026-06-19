"""
Phase-based pipeline runner for tabcloud-run.

Reads run.yml and executes the three-phase pipeline:
  Phase 1 — Extraction   (named jobs, fan-out, halt-at-barrier)
  Phase 2 — Transform    (named jobs, fan-out, halt-at-barrier)
  Phase 3 — Load         (named jobs, fan-out, destination-driven publish)

Features
--------
• Single YAML config entry point (run.yml). No INI.
• Interactive bootstrap when run.yml is absent or missing required values (TTY only).
• Static pre-flight validation before any data is touched.
• Optional pre-flight authentication (--skip-preflight to disable).
• --explain: print the fully-resolved plan without running.
• Secrets never in argv: literals are injected into the child env; refs are
  passed as-is for the component to resolve.

Usage:
  tabcloud-run -c run.yml [--explain] [--log-level INFO]
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional, Tuple

from runner.config import (
    RunConfig,
    ExtractionJobConfig,
    TransformJobConfig,
    LoadJobConfig,
    DestinationConfig,
    StorageConfig,
    BACKEND_S3,
    DATA_EVENT_LOGS,
    DATA_SNAPSHOTS,
    DATA_REFERENCE,
    load_run_config,
    resolve_window,
    sites_as_arg,
    RunConfigError,
    bundled_mappings_root,
)
from runner.validate import validate
from runner.plan import print_plan
from storage import get_extract_path, get_transform_path, get_loader_path

logger = logging.getLogger("runner")

DEFAULT_CONFIG_PATH = "./config/run.yml"


# ---------------------------------------------------------------------------
# Secret helpers (kept for backward-compat and tests)
# ---------------------------------------------------------------------------


def _pass_secret(
    args: List[str],
    env: dict,
    flag: str,
    ref_value: str,
    env_name: str,
) -> None:
    """Append *flag* + a secret reference to *args* without exposing literals.

    • If *ref_value* is a ``scheme:locator`` pointer reference (env/keyring/file/awssm/plain),
      pass it directly as the flag value — the child CLI will resolve it.
    • If *ref_value* is a bare literal, inject it into *env* under *env_name* and
      pass ``env:<env_name>`` so it never appears in the process list.
    """
    from common.secrets import is_reference
    if is_reference(ref_value):
        args += [flag, ref_value]
    else:
        env[env_name] = ref_value
        args += [flag, f"env:{env_name}"]


def _run_component(module: str, args_list: List[str], cwd: str, env: Optional[dict] = None) -> int:
    """Invoke ``python -m <module>`` as a subprocess; return its exit code."""
    cmd = [sys.executable, "-m", module, *args_list]
    child_env = {**os.environ, **(env or {})}
    return subprocess.run(cmd, cwd=cwd, env=child_env).returncode


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------


def _storage_args(cfg: RunConfig, tmp_files: List[str]) -> Tuple[List[str], List[str]]:
    """
    Return (common_args, cleanup_paths).

    common_args is a list of CLI flags to pass to every component for storage.
    cleanup_paths is a list of temp files to delete after the run.
    """
    st = cfg.storage
    if st.backend == BACKEND_S3:
        storage_yaml = _render_s3_storage_yaml(st)
        fd, tmp_path = tempfile.mkstemp(suffix=".yaml", prefix="tabcloud-storage-")
        os.close(fd)
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(storage_yaml)
        tmp_files.append(tmp_path)
        return ["--storage-config", tmp_path], []
    else:
        return ["--storage-path", st.path], []


def _render_s3_storage_yaml(st: StorageConfig) -> str:
    """Render a storage YAML file from the RunConfig S3 block."""
    try:
        import yaml
    except ImportError:
        raise ImportError("pyyaml is required. Install with: pip install pyyaml")

    s3 = st.s3
    creds = s3.credentials
    storage_dict: dict = {
        "backend": "s3",
        "s3": {
            "bucket": s3.bucket,
            "prefix": s3.prefix,
        },
    }
    if s3.region:
        storage_dict["s3"]["region"] = s3.region
    if s3.endpoint_url:
        storage_dict["s3"]["endpoint_url"] = s3.endpoint_url
    # credentials
    if creds.provider == "static":
        storage_dict["credentials"] = {
            "provider": "static",
            "access_key": creds.access_key,
            "secret_key": creds.secret_key,
        }
    elif creds.provider == "aws_profile":
        storage_dict["credentials"] = {
            "provider": "aws_profile",
            "profile": creds.profile,
        }
    elif creds.provider == "aws_secrets_manager":
        storage_dict["credentials"] = {
            "provider": "aws_secrets_manager",
            "secrets_manager": {
                "secret_id": creds.secrets_manager_id,
                "key_map": creds.secrets_manager_key_map,
            },
        }
    else:
        storage_dict["credentials"] = {"provider": "aws_default_chain"}
    return yaml.dump(storage_dict, default_flow_style=False, allow_unicode=True)


def _logging_args(cfg: RunConfig) -> List[str]:
    """Return log-dir and log-retention-days flags for all components."""
    args: List[str] = ["--log-retention-days", str(cfg.logging_cfg.retention_days)]
    log_dir = cfg.logging_cfg.dir or os.path.join(cfg.storage.path, "logs")
    args += ["--log-dir", log_dir]
    return args


def _component_log_level(cfg: RunConfig) -> str:
    level = cfg.logging_cfg.level.upper()
    # extractor uses "debug"/"info"; transformer/loader use "DEBUG"/"INFO"
    return "debug" if level == "DEBUG" else "info"


def _component_log_level_upper(cfg: RunConfig) -> str:
    return "DEBUG" if cfg.logging_cfg.level.upper() == "DEBUG" else "INFO"


# ---------------------------------------------------------------------------
# Phase 1 — Extraction
# ---------------------------------------------------------------------------


def _extraction_args(
    cfg: RunConfig,
    job: ExtractionJobConfig,
    storage_common: List[str],
) -> Tuple[List[str], dict]:
    """Build extractor CLI args for one named extraction job."""
    child_env: dict = {}
    args: List[str] = [
        "--pat-name", cfg.connection.token_name,
        "--api-url", cfg.connection.tcm_url,
        "--log-level", _component_log_level(cfg),
    ]
    args += storage_common
    args += _logging_args(cfg)

    if job.data == DATA_EVENT_LOGS:
        args += ["--dataset", "activitylog"]
        start, end = resolve_window(job)
        args += ["--start-time", start, "--end-time", end]
        if job.scope == "tenant":
            args.append("--retrieve-tenant-logs")
            # tenant scope: don't pass site-uris (tenant logs are global)
        else:
            args.append("--no-retrieve-tenant-logs")
            args += ["--site-uris", sites_as_arg(job)]
        if job.event_types:
            args += ["--event-type", ",".join(job.event_types)]

    elif job.data == DATA_SNAPSHOTS:
        args += ["--dataset", "snapshots"]
        args += ["--site-uris", sites_as_arg(job)]
        if job.entity_types:
            args += ["--entity-type", ",".join(job.entity_types)]
        args += [
            "--snapshot-selection", job.selection,
            "--snapshot-cadence", job.cadence,
            "--snapshot-lookback-intervals", str(job.lookback_intervals),
            "--snapshot-gap-action", job.gap_action,
        ]

    elif job.data == DATA_REFERENCE:
        # Reference entities are retrieved alongside an activitylog pass;
        # use a minimal rolling window so no new event files are downloaded
        # (skip-existing handles already-present files).
        args += ["--dataset", "activitylog"]
        start, end = resolve_window(job)
        args += ["--start-time", start, "--end-time", end]
        args.append("--no-retrieve-tenant-logs")
        args += ["--reference-entities", ",".join(job.entities)]

    else:
        raise ValueError(f"Unknown data type: {job.data!r}")

    # Tuning knobs (only when non-default)
    t = job.tuning
    if t.overwrite:
        args.append("--overwrite")
    if job.folder_layout:
        args += ["--extract-path-layout", job.folder_layout]

    # Numeric tuning overrides
    _default_tuning = {
        "wait_time": 60, "page_size": 1000, "download_threads": 10,
        "presigned_url_batch_size": 100, "presigned_url_timeout": 30,
        "max_chunk_hours": 168,
        "api_retry_max_total_wait": 300,
        "post_timeout_max_total_wait": 120,
        "download_retry_attempts": 3,
    }
    tuning_map = {
        "--wait-time": t.wait_time_seconds,
        "--page-size": t.page_size,
        "--download-threads": t.download_threads,
        "--presigned-url-batch-size": t.presigned_url_batch_size,
        "--presigned-url-timeout": t.presigned_url_timeout_seconds,
        "--max-chunk-hours": t.max_chunk_hours,
        "--api-retry-max-total-wait": t.api_retry_max_total_wait_seconds,
        "--post-timeout-max-total-wait": t.post_timeout_max_total_wait_seconds,
        "--download-retry-attempts": t.download_retry_attempts,
    }
    for flag, val in tuning_map.items():
        args += [flag, str(val)]

    # Secret (last, so it goes after all positional args)
    _pass_secret(args, child_env, "--pat-secret-ref", cfg.connection.token_secret, "_TABCLOUD_EXTRACTOR_PAT")
    return args, child_env


def run_extraction_job(cfg: RunConfig, name: str, job: ExtractionJobConfig, storage_common: List[str]) -> int:
    logger.info("Extraction '%s': data=%s", name, job.data)
    args, env = _extraction_args(cfg, job, storage_common)
    rc = _run_component("extractor", args, bundled_mappings_root(), env=env)
    if rc not in (0, 2):
        logger.error("Extraction '%s' failed (exit %d).", name, rc)
    elif rc == 2:
        logger.warning(
            "Extraction '%s' completed with degraded result (exit 2) → %s",
            name, get_extract_path(cfg.storage.path),
        )
    else:
        logger.info("Extraction '%s' succeeded → %s", name, get_extract_path(cfg.storage.path))
    return rc


# ---------------------------------------------------------------------------
# Phase 2 — Transform
# ---------------------------------------------------------------------------


def run_transform_job(cfg: RunConfig, name: str, job: TransformJobConfig, storage_common: List[str]) -> int:
    logger.info("Transform '%s': mapping=%s", name, job.mapping)
    args: List[str] = [
        "--mapping", job.mapping,
        "--log-level", _component_log_level_upper(cfg),
    ]
    args += storage_common
    args += _logging_args(cfg)
    rc = _run_component("transformer", args, bundled_mappings_root())
    if rc != 0:
        logger.error("Transform '%s' failed (exit %d).", name, rc)
    else:
        logger.info("Transform '%s' succeeded → %s", name, get_transform_path(cfg.storage.path))
    return rc


# ---------------------------------------------------------------------------
# Phase 3 — Load
# ---------------------------------------------------------------------------


def run_load_job(cfg: RunConfig, name: str, job: LoadJobConfig, storage_common: List[str]) -> int:
    logger.info("Load '%s': mapping=%s destination=%s", name, job.mapping, job.destination or "(local)")
    args: List[str] = [
        "--loader-mapping", job.mapping,
        "--log-level", _component_log_level_upper(cfg),
    ]
    args += storage_common
    args += _logging_args(cfg)
    child_env: dict = {}

    if job.destination:
        dest = cfg.destinations.get(job.destination)
        if dest is None:
            logger.error("Load '%s': destination '%s' not found.", name, job.destination)
            return 1
        args += [
            "--tableau-server-url", dest.server_url,
            "--tableau-token-name", dest.token_name,
        ]
        if dest.site:
            args += ["--tableau-site", dest.site]
        if dest.project:
            args += ["--tableau-project", dest.project]
        if dest.mode:
            args += ["--publish-mode", dest.mode]
        if job.datasource_name:
            args += ["--tableau-datasource-name", job.datasource_name]
        _pass_secret(
            args, child_env,
            "--tableau-token-secret-ref", dest.token_secret,
            "_TABCLOUD_PUBLISH_PAT",
        )
        if job.destination:
            args += ["--destination-name", job.destination]

    rc = _run_component("loader", args, bundled_mappings_root(), env=child_env)
    if rc != 0:
        logger.error("Load '%s' failed (exit %d).", name, rc)
    else:
        logger.info("Load '%s' succeeded → %s", name, get_loader_path(cfg.storage.path))
    return rc


# ---------------------------------------------------------------------------
# Phase runner (fan-out with halt-at-barrier)
# ---------------------------------------------------------------------------


def _run_phase(
    phase_name: str,
    jobs: dict,
    runner_fn,
) -> Tuple[int, List[str]]:
    """
    Run all jobs in *jobs* in declaration order.  Collect results.
    Returns (worst_exit_code, list_of_failed_names).

    Strategy: all siblings run; if any fail we halt BEFORE the next phase
    (halt-at-barrier) but do NOT kill siblings mid-flight.
    """
    results: Dict[str, int] = {}
    for name, job in jobs.items():
        try:
            rc = runner_fn(name, job)
        except Exception as exc:
            logger.exception("Phase %s / job '%s' raised an exception: %s", phase_name, name, exc)
            rc = 1
        results[name] = rc

    failed = [n for n, rc in results.items() if rc not in (0, 2)]
    degraded = [n for n, rc in results.items() if rc == 2]
    worst = 1 if failed else (2 if degraded else 0)

    if failed:
        logger.error(
            "Phase '%s': %d job(s) failed: %s — halting at barrier.",
            phase_name, len(failed), ", ".join(failed),
        )
    elif degraded:
        logger.warning(
            "Phase '%s': %d job(s) degraded (exit 2): %s — continuing.",
            phase_name, len(degraded), ", ".join(degraded),
        )
    return worst, failed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the full extraction → transform → load pipeline from a run.yml config."
        )
    )
    parser.add_argument(
        "--config", "-c",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to run.yml (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        default=False,
        help="Print the fully-resolved plan without executing, then exit 0.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        default=False,
        help=(
            "Skip pre-flight authentication checks. "
            "Use when you know credentials are valid and want to skip the round-trips."
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Runner log level (default: INFO).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )

    config_path = os.path.abspath(args.config)

    # -- Load or bootstrap config --
    from runner.bootstrap import maybe_bootstrap
    from runner.config import load_run_config, RunConfigError
    import warnings

    just_bootstrapped = False
    needs_bootstrap = not os.path.isfile(config_path)
    if needs_bootstrap:
        config_path = maybe_bootstrap(config_path)
        if config_path is None:
            return 1
        just_bootstrapped = True

    with warnings.catch_warnings(record=True) as w_list:
        warnings.simplefilter("always")
        try:
            cfg = load_run_config(config_path)
        except (RunConfigError, FileNotFoundError) as exc:
            logger.error("Could not load run.yml: %s", exc)
            # Check if bare-minimum values are missing and offer bootstrap
            config_path = maybe_bootstrap(config_path)
            if config_path is None:
                return 1
            just_bootstrapped = True
            try:
                cfg = load_run_config(config_path)
            except (RunConfigError, FileNotFoundError) as exc2:
                logger.error("Still cannot load config: %s", exc2)
                return 1

    for warning in w_list:
        logger.warning("Config warning: %s", warning.message)

    # -- Static validation --
    result = validate(cfg)
    if result.warnings:
        for w in result.warnings:
            logger.warning("Validation: %s", w)
    if not result.ok:
        logger.error("Config validation failed:")
        for e in result.errors:
            logger.error("  • %s", e)
        return 1

    # -- Explain mode --
    if args.explain:
        print_plan(cfg)
        return 0

    # -- Post-bootstrap plan confirmation (TTY only) --
    if just_bootstrapped and sys.stdin.isatty():
        print()
        print_plan(cfg)
        print()
        try:
            answer = input("  Proceed with this plan? [Y/n] ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            print()
            answer = "n"
        if answer not in ("", "y", "yes"):
            print()
            print(f"  Run aborted.  Edit your config and re-run:")
            print(f"    tabcloud-run -c {config_path}")
            print(f"  Use --explain to preview the plan without running.")
            return 0

    # -- Pre-flight auth --
    if not args.skip_preflight:
        from runner.preflight import run_preflight, PreflightError
        try:
            run_preflight(cfg)
        except PreflightError as exc:
            logger.error("%s", exc)
            return 1
    else:
        logger.info("Pre-flight skipped (--skip-preflight).")

    # -- Prepare storage args (shared across all phases) --
    tmp_files: List[str] = []
    storage_common, _ = _storage_args(cfg, tmp_files)

    started = time.time()
    try:
        exit_code = _execute_phases(cfg, storage_common)
    finally:
        # Clean up any temp storage config files
        for tmp in tmp_files:
            try:
                os.remove(tmp)
            except OSError:
                pass
        elapsed = int(time.time() - started)
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        logger.info("Execution finished (elapsed=%02d:%02d:%02d).", hours, minutes, seconds)

    return exit_code


def _execute_phases(cfg: RunConfig, storage_common: List[str]) -> int:
    """Run extraction → transform → load phases with halt-at-barrier semantics."""

    def _phase_elapsed(t0: float) -> str:
        s = int(time.time() - t0)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    # Phase 1 — Extraction
    if cfg.extraction:
        t1 = time.time()
        logger.info(
            "=== Phase 1 — Extraction (%d job(s)) ===", len(cfg.extraction)
        )
        worst, failed = _run_phase(
            "extraction",
            cfg.extraction,
            lambda name, job: run_extraction_job(cfg, name, job, storage_common),
        )
        if worst not in (0, 2):
            logger.error("Halting at extraction barrier — %d job(s) failed.", len(failed))
            return 1
        succeeded = len(cfg.extraction) - len(failed)
        logger.info(
            "=== Phase 1 — Extraction complete: %d/%d succeeded (elapsed %s) ===",
            succeeded, len(cfg.extraction), _phase_elapsed(t1),
        )
    else:
        logger.warning("No extraction jobs defined; skipping Phase 1.")

    # Phase 2 — Transform
    if cfg.transform:
        t2 = time.time()
        logger.info(
            "=== Phase 2 — Transform (%d job(s)) ===", len(cfg.transform)
        )
        worst, failed = _run_phase(
            "transform",
            cfg.transform,
            lambda name, job: run_transform_job(cfg, name, job, storage_common),
        )
        if worst not in (0, 2):
            logger.error("Halting at transform barrier — %d job(s) failed.", len(failed))
            return 1
        succeeded = len(cfg.transform) - len(failed)
        logger.info(
            "=== Phase 2 — Transform complete: %d/%d succeeded (elapsed %s) ===",
            succeeded, len(cfg.transform), _phase_elapsed(t2),
        )
    else:
        logger.warning("No transform jobs defined; skipping Phase 2.")

    # Phase 3 — Load
    if cfg.load:
        t3 = time.time()
        logger.info(
            "=== Phase 3 — Load (%d job(s)) ===", len(cfg.load)
        )
        worst, failed = _run_phase(
            "load",
            cfg.load,
            lambda name, job: run_load_job(cfg, name, job, storage_common),
        )
        if worst not in (0, 2):
            logger.error("Load phase: %d job(s) failed.", len(failed))
            return 1
        succeeded = len(cfg.load) - len(failed)
        logger.info(
            "=== Phase 3 — Load complete: %d/%d succeeded (elapsed %s) ===",
            succeeded, len(cfg.load), _phase_elapsed(t3),
        )
    else:
        logger.warning("No load jobs defined; skipping Phase 3.")

    logger.info("All phases completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
