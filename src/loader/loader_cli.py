"""
CLI for Loader: read Parquet, query state, load into target.
Mapping-driven: python -m loader --storage-path ./platform_data --loader-mapping mappings/loader/login_hyper.yaml [--ensure-upstream] [extractor args]
Legacy: python -m loader --storage-path ./platform_data --target postgres --mode bulk
"""

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import get_transform_path, get_loader_path, DEFAULT_STORAGE_BASE
from storage.run_logging import install_run_logging, resolve_log_dir
from loader.state import get_state
from loader.loader import load_parquet, load_hyper
from loader.loader_mapping import (
    load_loader_mapping,
    get_target_config,
    resolve_hyper_path,
    validate_loader_mapping,
    get_upstream_extractor_params,
)
from loader.orchestrate import run_upstream, get_required_pipelines_and_event_types
from loader.logging_utils import (
    configure_logging,
    get_logger,
    create_operation_id,
    log_operation,
)

logger = get_logger("cli")


def main():
    parser = argparse.ArgumentParser(
        description="Load Parquet data to a database or data warehouse. Use --loader-mapping for Hyper/Tableau load."
    )
    parser.add_argument(
        "--storage-path",
        default=DEFAULT_STORAGE_BASE,
        help=f"Storage base path (default: {DEFAULT_STORAGE_BASE}). Reads Parquet from {{path}}/transform, may write to {{path}}/loader. Use one base per tenant.",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Directory for this run's log file (default: {storage-path}/logs).",
    )
    parser.add_argument(
        "--log-retention-days",
        type=int,
        default=60,
        help="Delete *.log and *.log.N files in the log directory older than this many days (default: 60).",
    )
    parser.add_argument(
        "--storage-config",
        default=None,
        help="Path to storage config YAML. Overrides --storage-path for backend selection in upstream runs.",
    )
    parser.add_argument(
        "--loader-mapping",
        default=None,
        help="Path to loader mapping YAML (defines target type, tables, transformer mappings). When set, load is mapping-driven.",
    )
    parser.add_argument(
        "--ensure-upstream",
        action="store_true",
        help="When used with --loader-mapping: run extractor then transformer before load so required data exists.",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="Target name for legacy mode (e.g. postgres). Omit when using --loader-mapping.",
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "bulk"],
        default="bulk",
        help="Load mode for legacy mode: incremental or bulk.",
    )
    # TCM / extractor args: required when using --ensure-upstream to retrieve data. Not needed for Tableau publish.
    parser.add_argument(
        "--pat-name",
        default=None,
        help="TCM PAT name for the extractor. Required when using --ensure-upstream.",
    )
    parser.add_argument(
        "--pat-secret",
        default=None,
        help="TCM PAT secret for the extractor. Required when using --ensure-upstream.",
    )
    parser.add_argument(
        "--api-url",
        default=None,
        help="Tableau Cloud Manager (TCM) API URL for the extractor. Required when using --ensure-upstream.",
    )
    parser.add_argument("--start-time", default=None, help="Start time for extractor (ISO 8601). Required with --ensure-upstream.")
    parser.add_argument("--end-time", default=None, help="End time for extractor (ISO 8601). Required with --ensure-upstream.")
    parser.add_argument("--site-ids", default=None, help="Site IDs for extractor (comma, All, or None).")
    parser.add_argument("--no-retrieve-tenant-logs", action="store_true", help="Disable tenant-level logs in extractor.")
    parser.add_argument(
        "--extract-path-layout",
        default=None,
        metavar="LAYOUT",
        help="Extractor: date-first or schema-first (see extractor --help). Or set in loader mapping under upstream.extractor.",
    )
    # Tableau Server/Cloud publish: only when publishing the .hyper to Tableau. Not required for writing to .hyper in storage.
    parser.add_argument(
        "--tableau-server-url",
        default=None,
        help="Tableau Server/Cloud base URL. Only needed when publishing to Tableau.",
    )
    parser.add_argument(
        "--tableau-token-name",
        default=None,
        help="Tableau PAT name for publish. Only needed when publishing to Tableau.",
    )
    parser.add_argument(
        "--tableau-token-secret-ref",
        default=None,
        metavar="REF",
        help=(
            "Secret reference for the Tableau PAT secret (preferred). "
            "Accepts: keyring:SERVICE/USERNAME | env:VAR_NAME | file:/path | awssm:ID#KEY | plain:VALUE. "
            "Store your secret: tabcloud-secrets set tabcloud publish-pat  "
            "Then use: --tableau-token-secret-ref keyring:tabcloud/publish-pat"
        ),
    )
    parser.add_argument(
        "--tableau-token-secret",
        default=None,
        help=(
            "Tableau PAT secret as plaintext (DEPRECATED — visible in process list). "
            "Use --tableau-token-secret-ref instead."
        ),
    )
    parser.add_argument("--tableau-site", default=None, help="Tableau site name or LUID (when publishing).")
    parser.add_argument("--tableau-project", default=None, help="Tableau project name, path, or LUID (when publishing).")
    parser.add_argument(
        "--publish-mode",
        choices=["upsert", "hyper_update", "overwrite"],
        default=None,
        help=(
            "Tableau publish mode: upsert (default), hyper_update, or overwrite. "
            "Passed by the runner from the destination config; overrides the loader mapping."
        ),
    )
    parser.add_argument(
        "--tableau-datasource-name",
        default=None,
        help="Published data source name on Tableau (overrides loader mapping's datasource name).",
    )
    parser.add_argument(
        "--destination-name",
        default=None,
        help=(
            "Name of the destination (from run.yml destinations map). "
            "Used to track per-destination published_through watermarks for self-healing publish."
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO).",
    )
    args = parser.parse_args()

    log_dir_abs = resolve_log_dir(args.storage_path, args.log_dir)
    install_run_logging("loader", log_dir_abs, args.log_retention_days)

    level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    configure_logging(level=level)

    project_root = os.getcwd()

    if args.loader_mapping:
        mapping_path = args.loader_mapping
        if not os.path.isabs(mapping_path):
            mapping_path = os.path.join(project_root, mapping_path)
        if not os.path.isfile(mapping_path):
            logger.error("Loader mapping not found: %s", mapping_path)
            sys.exit(1)
        logger.info("Loading loader mapping from %s", mapping_path)
        loader_config = load_loader_mapping(mapping_path)
        errors = validate_loader_mapping(loader_config, project_root)
        if errors:
            for e in errors:
                logger.error("Validation error: %s", e)
            sys.exit(1)
        logger.info("Loader mapping validated successfully")
        transform_dir = get_transform_path(args.storage_path)
        if not os.path.isdir(transform_dir):
            os.makedirs(transform_dir, exist_ok=True)
        if args.ensure_upstream:
            logger.info("Running upstream (extractor then transformer)")
            from common.secrets import resolve_secret, SecretResolutionError
            upstream_defaults = get_upstream_extractor_params(loader_config)
            # Resolve upstream PAT secret (supports secret references in loader mapping).
            raw_pat_secret = args.pat_secret or upstream_defaults.get("pat_secret", "")
            if raw_pat_secret:
                try:
                    raw_pat_secret = resolve_secret(raw_pat_secret)
                except SecretResolutionError as exc:
                    logger.error("Failed to resolve upstream extractor PAT secret: %s", exc)
                    sys.exit(1)
            extractor_params = {
                "pat_name": args.pat_name or upstream_defaults.get("pat_name"),
                "pat_secret": raw_pat_secret,
                "api_url": args.api_url or upstream_defaults.get("api_url"),
                "start_time": args.start_time or upstream_defaults.get("start_time"),
                "end_time": args.end_time or upstream_defaults.get("end_time"),
                "site_ids": args.site_ids if args.site_ids is not None else upstream_defaults.get("site_ids"),
                "retrieve_tenant_logs": not getattr(args, "no_retrieve_tenant_logs", False),
                "extract_path_layout": args.extract_path_layout or upstream_defaults.get("extract_path_layout"),
            }
            if not extractor_params.get("pat_name") or not extractor_params.get("pat_secret") or not extractor_params.get("api_url"):
                logger.error("When using --ensure-upstream, provide --pat-name, --pat-secret, and --api-url (or set in loader mapping upstream.extractor).")
                sys.exit(1)
            if not extractor_params.get("start_time") or not extractor_params.get("end_time"):
                logger.error("When using --ensure-upstream, provide --start-time and --end-time (or set in loader mapping upstream.extractor).")
                sys.exit(1)
            ok = run_upstream(
                loader_config,
                args.storage_path,
                extractor_params,
                project_root,
                storage_config_path=getattr(args, "storage_config", None),
                log_dir=log_dir_abs,
                log_retention_days=args.log_retention_days,
            )
            if not ok:
                logger.error("Upstream run (extractor or transformer) failed.")
                sys.exit(1)
            logger.info("Upstream run completed successfully")
        target = get_target_config(loader_config, args.storage_path)
        # Publish options only when the runner explicitly passes Tableau args.
        # The loader mapping's embedded `publish:` block is intentionally ignored here;
        # publishing is destination-driven from run.yml to avoid accidental publishes to
        # placeholder or stale URLs.
        publish_options = None
        if args.tableau_server_url:
            from common.secrets import resolve_secret, SecretResolutionError
            # Resolve the publish PAT secret from runner-passed args only.
            # Priority: --tableau-token-secret-ref (ref) > --tableau-token-secret (deprecated literal).
            # The loader mapping's embedded publish block is intentionally not consulted.
            raw_token_secret = args.tableau_token_secret_ref or args.tableau_token_secret or ""
            if args.tableau_token_secret and not args.tableau_token_secret_ref:
                logger.warning(
                    "--tableau-token-secret passes the secret as a plaintext argument. "
                    "Use --tableau-token-secret-ref with a keyring: or env: reference instead. "
                    "Store your secret: tabcloud-secrets set tabcloud publish-pat"
                )
            resolved_token_secret = None
            if raw_token_secret:
                try:
                    resolved_token_secret = resolve_secret(raw_token_secret)
                except SecretResolutionError as exc:
                    logger.error("Failed to resolve Tableau publish token secret: %s", exc)
                    sys.exit(1)
            publish_options = {
                "server_url": args.tableau_server_url,
                "token_name": args.tableau_token_name,
                "token_secret": resolved_token_secret,
                "site": args.tableau_site or "",
                "project": args.tableau_project or "",
                "datasource_name": args.tableau_datasource_name or None,
                "description": None,
                "destination_name": getattr(args, "destination_name", None),
                "publish_mode_override": getattr(args, "publish_mode", None),
            }
            if publish_options.get("server_url") and publish_options.get("token_name") and publish_options.get("token_secret"):
                logger.info("Tableau publish enabled (server: %s)", publish_options.get("server_url", ""))
            else:
                publish_options = None
                logger.warning("Tableau publish args present but missing server_url, token_name, or token_secret; writing to .hyper only.")
        op_id = create_operation_id()
        log_operation(
            op_id, "load_hyper", "start",
            inputs={"storage_path": args.storage_path, "loader_mapping": args.loader_mapping},
        )
        start = time.perf_counter()
        n = load_hyper(
            loader_config,
            args.storage_path,
            transform_dir,
            project_root=project_root,
            publish_options=publish_options,
        )
        duration_ms = int((time.perf_counter() - start) * 1000)
        # load_hyper returns -1 to signal the local Hyper was written but the Tableau
        # publish failed; surface that as a non-zero exit so callers/orchestrators notice.
        success = n >= 0
        log_operation(
            op_id, "load_hyper", "complete",
            inputs={"storage_path": args.storage_path, "loader_mapping": args.loader_mapping},
            result={"success": success, "rows_written": n},
            duration=duration_ms,
        )
        if not success:
            logger.error(
                "Hyper written locally, but Tableau publish failed; datasource may be stale. "
                "See the publish error logged above."
            )
            sys.exit(1)
        hyper_final = resolve_hyper_path(target.get("hyper_path", ""), args.storage_path)
        logger.info("Loaded %d rows to Hyper → %s", n, hyper_final)
        return

    # Legacy path: list Parquet, target, mode
    transform_dir = get_transform_path(args.storage_path)
    loader_dir = get_loader_path(args.storage_path)
    if not os.path.isdir(transform_dir):
        logger.error("Transform directory not found: %s", transform_dir)
        sys.exit(1)
    parquet_files = [
        os.path.join(transform_dir, f)
        for f in os.listdir(transform_dir)
        if f.endswith(".parquet")
    ]
    if not parquet_files:
        logger.warning("No Parquet files found to load in %s", transform_dir)
        return
    target_config = {"name": args.target} if args.target else {}
    state = get_state(target_config) if args.target else None
    if args.target:
        n = load_parquet(parquet_files, target_config, mode=args.mode)
        logger.info("Loaded %d rows into %s (%s mode).", n, args.target, args.mode)
    else:
        logger.info("Dry run: would load %d file(s) from %s (specify --target to load).", len(parquet_files), transform_dir)


if __name__ == "__main__":
    main()
