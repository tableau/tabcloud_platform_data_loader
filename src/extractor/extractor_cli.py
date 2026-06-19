"""
CLI for Extractor: retrieve and replicate TCM Platform Data API files to extract storage.

Supports two dataset types:
  - activitylog (default): activity-log event files — site and tenant scope.
  - snapshots:             entity-snapshot Parquet files — site-scope only at launch.
  - both:                  runs an activitylog pass then a snapshots pass in one invocation.

Usage:
    python -m extractor --pat-name ... --pat-secret ... --api-url ...
        [--dataset {activitylog,snapshots,both}]
        [--start-time ...] [--end-time ...]      # required for activitylog; optional for snapshots
        [--event-type ...]                        # filter event types (activitylog)
        [--entity-type ...]                       # filter entity types (snapshots)
        [--snapshot-selection {latest,all}]
        [--snapshot-cadence {hourly,daily}]
        [--snapshot-lookback-intervals N]
        [--snapshot-gap-action {warn,error,ignore}]
        [--storage-path ./platform_data]

Exit status:
    0 = all required downloads completed
    2 = degraded (some paths missing)
    1 = error (authentication, validation, unhandled exception)
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

from common.dataset_profile import get_profile, ACTIVITYLOG, SNAPSHOTS
from storage import (
    LocalStorage,
    get_extract_path,
    DEFAULT_STORAGE_BASE,
    load_storage_config,
    create_component_backend,
)
from storage.run_logging import install_run_logging, resolve_log_dir
from extractor.path_layout import (
    ExtractPathLayout,
    assert_layout_matches_storage,
    detect_layout_from_storage,
)
from extractor.http_retry import tcm_api_error_from_exception, print_tcm_api_error_body
from extractor.retriever import (
    LogRetriever,
    create_operation_id,
    parse_site_ids,
    parse_site_uris,
    parse_event_types,
    generate_time_chunks,
    compute_span_hours,
    MAX_CHUNK_HOURS,
)

logger = logging.getLogger("extractor.cli")


def _is_site_active(site: dict) -> bool:
    """Return True if the site should be queried for data.

    The TCM list-sites response includes a ``status`` field ("Active" / "Suspended")
    and a ``suspendedDateTime`` field that is non-null when the site has been suspended.
    We treat a site as active only when:
      - ``status`` is absent, empty, or explicitly "Active" (case-insensitive), AND
      - ``suspendedDateTime`` is absent or null.
    Any other combination (status="Suspended", suspendedDateTime set, etc.) means the
    site is not active and should be skipped.
    """
    status = (site.get("status") or "").strip()
    suspended_at = site.get("suspendedDateTime")
    if suspended_at:
        return False
    if status and status.lower() not in ("active", "unknown", ""):
        return False
    return True


def _build_retriever(args, storage, path_layout, profile, start_time, end_time):
    """Construct a LogRetriever for the given profile and time window."""
    return LogRetriever(
        pat_name=args.pat_name,
        pat_secret=args.pat_secret,
        api_url=args.api_url,
        site_ids=[],  # populated later by the site-resolution block
        start_time=start_time,
        end_time=end_time,
        storage_backend=storage,
        wait_time=args.wait_time,
        page_size=args.page_size,
        download_threads=args.download_threads,
        max_chunk_hours=args.max_chunk_hours,
        presigned_url_batch_size=args.presigned_url_batch_size,
        presigned_url_timeout=args.presigned_url_timeout,
        log_level=args.log_level,
        overwrite=args.overwrite,
        path_layout=path_layout,
        api_retry_max_total_wait_seconds=args.api_retry_max_total_wait,
        post_timeout_max_total_wait=args.post_timeout_max_total_wait,
        download_max_attempts=args.download_retry_attempts,
        dataset_profile=profile,
        snapshot_selection=args.snapshot_selection,
        snapshot_cadence=args.snapshot_cadence,
        snapshot_lookback_intervals=args.snapshot_lookback_intervals,
        snapshot_gap_action=args.snapshot_gap_action,
        allow_tenant_snapshots=args.allow_tenant_snapshots,
    )


def _resolve_sites(args, retriever):
    """
    Resolve site scope from --site-uris / --site-ids.
    Updates retriever.site_ids and retriever.site_uri_by_id.
    Returns the final site_ids list.
    """
    site_input = args.site_uris or args.site_ids
    if not site_input:
        logger.info(
            "No site scope provided. "
            "Only tenant-scope data will be retrieved (if --retrieve-tenant-logs is set)."
        )
        return []

    try:
        if args.site_uris:
            parsed = parse_site_uris(site_input)
        else:
            parsed = parse_site_ids(site_input)
    except ValueError as e:
        logger.error("%s", e)
        sys.exit(1)

    if parsed == "none":
        logger.info("'None' specified. Site-scope retrieval skipped.")
        return []

    if parsed == "all" or (args.site_uris and args.site_uris.strip().lower() != "none"):
        # Always need to list sites when resolving by URI or "all"
        if not retriever.session_token and not retriever._login(create_operation_id()):
            print_tcm_api_error_body(retriever._last_tcm_api_error)
            logger.error("Failed to authenticate. Cannot retrieve site list.")
            sys.exit(1)
        all_sites = retriever._list_tenant_sites(create_operation_id())
        retriever.site_uri_by_id = {
            str(s.get("siteUUID", "")).strip(): str(s.get("contentUrl", "")).strip()
            for s in all_sites
            if s.get("siteUUID") and s.get("contentUrl") and _is_site_active(s)
        }

    if parsed == "all":
        active_sites = [s for s in all_sites if _is_site_active(s)]
        skipped = [s for s in all_sites if not _is_site_active(s)]
        if skipped:
            logger.info(
                "Skipping %d inactive/suspended site(s): %s",
                len(skipped),
                ", ".join(s.get("contentUrl") or s.get("siteUUID") or "(unknown)" for s in skipped),
            )
        site_ids = [s.get("siteUUID") for s in active_sites if s.get("siteUUID")]
        logger.info("Found %d active site(s) in tenant (of %d total).", len(site_ids), len(all_sites))
        for idx, s in enumerate(active_sites, start=1):
            logger.info("  %d. uri=%s, id=%s", idx,
                        s.get("contentUrl") or "(missing)", s.get("siteUUID") or "(missing)")
    elif args.site_uris:
        # Resolve URIs → UUIDs
        all_sites = retriever._list_tenant_sites(create_operation_id()) if "all_sites" not in dir() else all_sites
        site_uuid_by_uri = {
            str(s.get("contentUrl", "")).strip(): str(s.get("siteUUID", "")).strip()
            for s in all_sites if s.get("contentUrl") and s.get("siteUUID")
        }
        invalid_uris = [u for u in parsed if u not in site_uuid_by_uri]
        if invalid_uris:
            logger.error("The following site URIs are not valid for this tenant: %s", ", ".join(invalid_uris))
            logger.info("Valid site URIs: %s", ", ".join(sorted(site_uuid_by_uri.keys())))
            sys.exit(1)
        # Warn if any explicitly requested sites are inactive.
        site_by_uri = {str(s.get("contentUrl", "")).strip(): s for s in all_sites}
        inactive_requested = [u for u in parsed if not _is_site_active(site_by_uri.get(u, {}))]
        if inactive_requested:
            logger.warning(
                "The following requested site(s) are inactive/suspended and will be skipped: %s",
                ", ".join(inactive_requested),
            )
        active_parsed = [u for u in parsed if u not in inactive_requested]
        site_ids = [site_uuid_by_uri[u] for u in active_parsed]
        retriever.site_uri_by_id = {site_uuid_by_uri[u]: u for u in active_parsed}
        logger.info("Resolved %d active site URI(s) to IDs (of %d requested).", len(site_ids), len(parsed))
    else:
        # Legacy --site-ids path (UUIDs directly)
        site_ids = parsed
        logger.info("Using %d explicit site ID(s).", len(site_ids))
        if not retriever.session_token and not retriever._login(create_operation_id()):
            print_tcm_api_error_body(retriever._last_tcm_api_error)
            logger.error("Failed to authenticate. Cannot validate site IDs.")
            sys.exit(1)
        all_sites = retriever._list_tenant_sites(create_operation_id())
        site_by_id = {s.get("siteUUID"): s for s in all_sites}
        valid_ids = set(site_by_id.keys())
        invalid = [sid for sid in site_ids if sid not in valid_ids]
        if invalid:
            logger.error("The following site IDs are not valid: %s", ", ".join(invalid))
            sys.exit(1)
        inactive = [sid for sid in site_ids if not _is_site_active(site_by_id.get(sid, {}))]
        if inactive:
            logger.warning(
                "The following requested site(s) are inactive/suspended and will be skipped: %s",
                ", ".join(inactive),
            )
        site_ids = [sid for sid in site_ids if sid not in inactive]

    retriever.site_ids = site_ids
    return site_ids


def _run_single_pass(
    args, storage, path_layout, profile, start_time, end_time, filter_values,
    retrieve_tenant, list_mode=False, list_mode_name="values",
):
    """Run one extraction pass for a single profile.

    Returns the ExtractionResult (or a partition-value list in list_mode), or None on error.
    """
    retriever = _build_retriever(args, storage, path_layout, profile, start_time, end_time)
    _resolve_sites(args, retriever)

    if list_mode:
        result = retriever.list_partition_values(
            filter_values=filter_values,
            retrieve_tenant=retrieve_tenant,
        )
        if result is None:
            return None
        logger.info(
            "Found %d unique %s value(s) for %s dataset:",
            len(result), profile.path_key, profile.name,
        )
        for idx, v in enumerate(result, start=1):
            logger.info("  %d. %s", idx, v)
        return result

    try:
        return retriever.run_incremental(
            event_types=filter_values,
            retrieve_tenant_logs=retrieve_tenant,
        )
    except Exception as exc:
        logger.error("[%s] Error: %s", profile.name, exc)
        if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
            print_tcm_api_error_body(tcm_api_error_from_exception(exc), file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Tableau Cloud Manager Platform Data Retriever",
        epilog=(
            "Exit status: 0 = all required downloads completed; "
            "2 = incomplete (degraded, some paths missing); "
            "1 = error (e.g. authentication, validation, unhandled exception)."
        ),
    )

    # ---- Identity / connection ----
    parser.add_argument("--pat-name", required=True, help="Personal Access Token name.")
    parser.add_argument(
        "--pat-secret-ref",
        default=None,
        metavar="REF",
        help=(
            "Secret reference for the PAT secret (preferred). "
            "Accepts: keyring:SERVICE/USERNAME | env:VAR_NAME | file:/path | awssm:ID#KEY | plain:VALUE. "
            "Store your secret first: tabcloud-secrets set tabcloud extractor-pat  "
            "Then use: --pat-secret-ref keyring:tabcloud/extractor-pat  "
            "Run 'tabcloud-secrets doctor' to verify your keyring backend."
        ),
    )
    parser.add_argument(
        "--pat-secret",
        default=None,
        help=(
            "PAT secret as a plaintext value (DEPRECATED — secret appears in process list). "
            "Use --pat-secret-ref instead."
        ),
    )
    parser.add_argument("--api-url", required=True, help="Tableau Cloud Manager API URL.")

    # ---- Dataset selection ----
    parser.add_argument(
        "--dataset",
        choices=["activitylog", "snapshots", "both"],
        default="activitylog",
        help=(
            "Dataset to retrieve: 'activitylog' (default), 'snapshots', or 'both'. "
            "'both' runs an activitylog pass then a snapshots pass in one invocation."
        ),
    )

    # ---- Time window (required for activitylog; optional for snapshots via cadence) ----
    parser.add_argument(
        "--start-time",
        default=None,
        help=(
            "Start time for retrieval (ISO 8601). Required when --dataset is 'activitylog' or 'both'. "
            "For 'snapshots' only, the cadence-aware default window is used when omitted."
        ),
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="End time for retrieval (ISO 8601). Same requirements as --start-time.",
    )

    # ---- Filter values ----
    parser.add_argument(
        "--event-type",
        default=None,
        help="Comma-delimited eventType values to filter activity-log retrieval. Omit for all types.",
    )
    parser.add_argument(
        "--entity-type",
        default=None,
        help=(
            "Comma-delimited entityType values to filter snapshot retrieval. "
            "Values are API-defined, case-sensitive, and must match the entityType= "
            "path segments returned by the Tableau Cloud Manager API exactly. "
            "The API emits snake_case values (e.g. 'data_connections,datasources,users'). "
            "Use --list-entity-types to see the exact values for your tenant. "
            "Omit to retrieve all available entity types."
        ),
    )

    # ---- Site / tenant scope ----
    parser.add_argument(
        "--site-uris",
        help="Comma-delimited site contentUrl values, 'All' for all sites, or 'None' to skip.",
    )
    parser.add_argument(
        "--site-ids",
        help="(Deprecated: use --site-uris) Comma-delimited site UUIDs, 'All', or 'None'.",
    )
    parser.add_argument(
        "--list-sites",
        action="store_true",
        default=False,
        help="Print available sites for this tenant and exit (requires auth).",
    )
    parser.add_argument(
        "--retrieve-tenant-logs",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Retrieve activity logs from the tenant scope in addition to sites (default: False). "
            "Use --retrieve-tenant-logs to enable. "
            "NOTE: this default changed from True to False. If you relied on implicit tenant "
            "retrieval in a previous version, add --retrieve-tenant-logs explicitly."
        ),
    )

    # ---- Discovery helpers ----
    parser.add_argument(
        "--list-events",
        action="store_true",
        default=False,
        help="List unique eventType values via API metadata (no downloads), then exit.",
    )
    parser.add_argument(
        "--list-entities",
        action="store_true",
        default=False,
        help="List unique entityType values available in the snapshots API, then exit.",
    )

    # ---- Snapshot-specific options ----
    parser.add_argument(
        "--snapshot-selection",
        choices=["latest", "all"],
        default="latest",
        help=(
            "How to select snapshot files when multiple are available: "
            "'latest' (default) keeps only the newest per entity type; "
            "'all' downloads every available snapshot file."
        ),
    )
    parser.add_argument(
        "--snapshot-cadence",
        choices=["hourly", "daily"],
        default="hourly",
        help=(
            "Expected cadence of snapshot delivery for cadence-aware window calculations "
            "and gap detection (default: hourly)."
        ),
    )
    parser.add_argument(
        "--snapshot-lookback-intervals",
        type=int,
        default=4,
        help=(
            "Number of cadence intervals to look back when no explicit --start-time is given "
            "(default: 4; hourly → 4 h, daily → 4 d). Increase to catch late deliveries."
        ),
    )
    parser.add_argument(
        "--snapshot-gap-action",
        choices=["warn", "error", "ignore"],
        default="warn",
        help=(
            "What to do when expected snapshots are missing or stale within the lookback window: "
            "'warn' (default) logs a warning; 'error' fails the run; 'ignore' suppresses."
        ),
    )
    parser.add_argument(
        "--allow-tenant-snapshots",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,  # Forward-compat escape hatch; not yet available from the API
    )

    # ---- Reference data (tenant-wide TCM REST API lists) ----
    parser.add_argument(
        "--reference-entities",
        default=None,
        metavar="ENTITIES",
        help=(
            "Comma-separated tenant-wide reference datasets to pull from the TCM REST API "
            "after the main extraction pass. "
            "Valid values: 'tenant_sites', 'tenant_users'. "
            "Example: --reference-entities tenant_sites,tenant_users. "
            "Omit to skip reference data retrieval. "
            "Output is written to extract/reference/{entity}/pulled=<ISO8601>/ as JSON pages "
            "for downstream transformer normalization."
        ),
    )

    # ---- Performance / tuning ----
    parser.add_argument("--wait-time", type=int, default=60)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--download-threads", type=int, default=10)
    parser.add_argument("--presigned-url-batch-size", type=int, default=100)
    parser.add_argument("--presigned-url-timeout", type=int, default=30)
    parser.add_argument(
        "--max-chunk-hours",
        type=int,
        default=MAX_CHUNK_HOURS,
        help=f"Max hours per API request chunk (default: {MAX_CHUNK_HOURS}).",
    )
    parser.add_argument("--api-retry-max-total-wait", type=int, default=300)
    parser.add_argument("--post-timeout-max-total-wait", type=int, default=120)
    parser.add_argument("--download-retry-attempts", type=int, default=3)

    # ---- Storage / layout ----
    parser.add_argument(
        "--log-level",
        choices=["info", "debug"],
        default="info",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--storage-path",
        default=DEFAULT_STORAGE_BASE,
        help="Storage base path (default: %(default)s).",
    )
    parser.add_argument("--log-dir", default=None)
    parser.add_argument("--log-retention-days", type=int, default=60)
    parser.add_argument("--storage-config", default=None)
    parser.add_argument(
        "--extract-path-layout",
        metavar="LAYOUT",
        help=(
            "How to lay out files under extract/: 'date-first' or 'schema-first'. "
            "If omitted, auto-detects from existing files or defaults to date-first."
        ),
    )
    parser.add_argument("--force", "-f", action="store_true", default=False)

    args = parser.parse_args()

    # ---- Logging setup ----
    log_dir_abs = resolve_log_dir(args.storage_path, args.log_dir)
    install_run_logging("extractor", log_dir_abs, args.log_retention_days)
    level = logging.DEBUG if str(args.log_level).lower() == "debug" else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )

    # ---- Resolve PAT secret ----
    # Prefer --pat-secret-ref (supports keyring/env/file/awssm references).
    # Fall back to --pat-secret (plaintext, deprecated).
    if args.pat_secret_ref:
        from common.secrets import resolve_secret, SecretResolutionError
        try:
            args.pat_secret = resolve_secret(args.pat_secret_ref)
        except SecretResolutionError as exc:
            logger.error("Failed to resolve PAT secret: %s", exc)
            sys.exit(1)
    elif args.pat_secret:
        logger.warning(
            "--pat-secret passes the secret as a plaintext argument (visible in process list). "
            "Use --pat-secret-ref with a keyring: or env: reference instead. "
            "Store your secret: tabcloud-secrets set tabcloud extractor-pat"
        )
    else:
        logger.error(
            "No PAT secret provided. Use --pat-secret-ref (recommended) or --pat-secret.\n"
            "Quick start:\n"
            "  pip install -e .[secrets]\n"
            "  tabcloud-secrets set tabcloud extractor-pat\n"
            "  Then pass: --pat-secret-ref keyring:tabcloud/extractor-pat\n"
            "Run 'tabcloud-secrets doctor' to verify your keyring backend."
        )
        sys.exit(1)

    # ---- Validate max_chunk_hours ----
    if args.max_chunk_hours < 1 or args.max_chunk_hours > MAX_CHUNK_HOURS:
        logger.error("--max-chunk-hours must be between 1 and %s.", MAX_CHUNK_HOURS)
        sys.exit(1)

    # ---- Validate time window requirements ----
    needs_log_window = args.dataset in ("activitylog", "both")
    if needs_log_window and (not args.start_time or not args.end_time):
        logger.error(
            "--start-time and --end-time are required when --dataset is '%s'.", args.dataset
        )
        sys.exit(1)

    if args.start_time and args.end_time:
        span_hours = compute_span_hours(args.start_time, args.end_time)
        if span_hours > args.max_chunk_hours:
            chunk_count = len(generate_time_chunks(
                args.start_time, args.end_time, max_hours=args.max_chunk_hours,
            ))
            logger.warning(
                "The requested date range spans %.1f days (%.1f hours).",
                span_hours / 24, span_hours,
            )
            logger.warning(
                "This will require %d API request chunk(s) of up to %d hours each.",
                chunk_count, args.max_chunk_hours,
            )
            if not args.force:
                answer = input("Do you want to continue? [y/N]: ").strip().lower()
                if answer not in ("y", "yes"):
                    logger.info("Aborted by user.")
                    sys.exit(0)

    # ---- Storage setup ----
    storage_config = load_storage_config(args.storage_config) if args.storage_config else None
    if storage_config:
        storage = create_component_backend(storage_config, storage_path=args.storage_path, component="extract")
    else:
        storage = LocalStorage(base_path=get_extract_path(args.storage_path))

    if args.extract_path_layout is not None:
        try:
            path_layout = ExtractPathLayout.from_cli(args.extract_path_layout)
        except ValueError as e:
            logger.error("%s", e)
            sys.exit(1)
        try:
            assert_layout_matches_storage(path_layout, storage)
        except ValueError as e:
            logger.error("%s", e)
            sys.exit(1)
    else:
        path_layout = detect_layout_from_storage(storage) or ExtractPathLayout.DATE_FIRST
    logger.info("Extract path layout: %s", path_layout.value)

    # ---- --list-sites ----
    if args.list_sites:
        retriever = _build_retriever(
            args, storage, path_layout, ACTIVITYLOG,
            args.start_time or "", args.end_time or "",
        )
        if not retriever.session_token and not retriever._login(create_operation_id()):
            print_tcm_api_error_body(retriever._last_tcm_api_error)
            logger.error("Failed to authenticate. Cannot retrieve site list.")
            sys.exit(1)
        all_sites = retriever._list_tenant_sites(create_operation_id())
        if not all_sites:
            logger.info("No sites found for this tenant.")
            sys.exit(0)
        logger.info(f"{'#':<4} {'contentUrl':<40} {'siteUUID'}")
        logger.info(f"{'—'*3:<4} {'—'*38:<40} {'—'*36}")
        for idx, site in enumerate(all_sites, start=1):
            uri = str(site.get("contentUrl") or "").strip() or "(missing)"
            sid = str(site.get("siteUUID") or "").strip() or "(missing)"
            logger.info(f"{idx:<4} {uri:<40} {sid}")
        sys.exit(0)

    # ---- Determine which datasets to run ----
    datasets_to_run = []
    if args.dataset == "both":
        datasets_to_run = [("activitylog", ACTIVITYLOG), ("snapshots", SNAPSHOTS)]
    else:
        profile = get_profile(args.dataset)
        datasets_to_run = [(args.dataset, profile)]

    # ---- --list-events or --list-entities ----
    if args.list_events or args.list_entities:
        if args.list_events:
            logger.info("Listing unique eventType values for activitylog...")
            _run_single_pass(
                args, storage, path_layout, ACTIVITYLOG,
                args.start_time, args.end_time,
                filter_values=parse_event_types(args.event_type),
                retrieve_tenant=args.retrieve_tenant_logs,
                list_mode=True,
            )
        if args.list_entities:
            logger.info("Listing unique entityType values for snapshots...")
            _run_single_pass(
                args, storage, path_layout, SNAPSHOTS,
                args.start_time, args.end_time,
                filter_values=parse_event_types(args.entity_type),
                retrieve_tenant=False,
                list_mode=True,
            )
        sys.exit(0)

    # ---- Main extraction runs ----
    any_degraded = False
    any_failed = False

    for dataset_name, profile in datasets_to_run:
        logger.info("=== Starting %s extraction pass ===", dataset_name)

        is_snapshots = (profile is SNAPSHOTS or profile.name == "snapshots")
        filter_values = (
            parse_event_types(args.entity_type)
            if is_snapshots
            else parse_event_types(args.event_type)
        )
        start_time = args.start_time if not is_snapshots else args.start_time  # can be None for snapshots
        end_time = args.end_time if not is_snapshots else args.end_time
        retrieve_tenant = args.retrieve_tenant_logs and not is_snapshots

        result = _run_single_pass(
            args, storage, path_layout, profile,
            start_time, end_time,
            filter_values=filter_values,
            retrieve_tenant=retrieve_tenant,
        )

        if result is None:
            if args.dataset == "both":
                # In 'both' mode, a snapshot failure is degraded, not a hard stop.
                if is_snapshots:
                    logger.warning(
                        "Snapshots extraction encountered an error. "
                        "Activity-log data may still have been retrieved successfully."
                    )
                    any_degraded = True
                else:
                    any_failed = True
            else:
                sys.exit(1)
        elif hasattr(result, "status"):
            if result.status == "failed":
                if args.dataset == "both" and is_snapshots:
                    # Snapshot-only failure in 'both' mode: degrade, not hard fail.
                    any_degraded = True
                else:
                    any_failed = True
            elif result.status == "degraded":
                any_degraded = True

    # ---- Reference data pass (tenant-wide TCM REST API lists) ----
    if getattr(args, "reference_entities", None):
        from common.reference_entity import parse_reference_entities
        try:
            ref_entities = parse_reference_entities(args.reference_entities)
        except ValueError as e:
            logger.error("Invalid --reference-entities: %s", e)
            sys.exit(1)

        if ref_entities:
            logger.info("=== Starting reference data pass: %s ===",
                        ", ".join(e.name for e in ref_entities))
            # Build a minimal retriever just for auth + the reference pass.
            ref_retriever = _build_retriever(
                args, storage, path_layout, ACTIVITYLOG,
                args.start_time or "", args.end_time or "",
            )
            if not ref_retriever.session_token and not ref_retriever._login(create_operation_id()):
                print_tcm_api_error_body(ref_retriever._last_tcm_api_error)
                logger.error("Failed to authenticate for reference data pass.")
                any_failed = True
            else:
                try:
                    ref_results = ref_retriever.run_reference_pass(ref_entities)
                    for r in ref_results:
                        logger.info(
                            "Reference '%s': %d items in %d page(s) → %s",
                            r.entity.name, r.total_items, r.pages_written, r.output_dir,
                        )
                except Exception as exc:
                    logger.error("Reference data pass failed: %s", exc)
                    any_degraded = True

    if any_failed:
        sys.exit(1)
    if any_degraded:
        logger.warning(
            "Not all required files are present in storage. "
            "Check logs for failed_paths and degraded passes."
        )
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
