"""
CLI for Transformer: read extract storage, apply mapping(s), write Parquet/CSV.

The --mapping flag is now repeatable; supply it once per mapping file to process
multiple mappings in a single invocation (e.g. an event mapping + snapshot mappings):

    python -m transformer
        --storage-path ./platform_data
        --mapping mappings/transformer/generic_activity_log_auto.yaml
        --mapping mappings/transformer/entity_snapshot_auto.yaml

All mappings share the same storage backends; each mapping writes its own output
file(s) to the transform directory.  Mappings are run sequentially by default.
"""

import argparse
import logging
import os
import sys

# Run from project root so mappings/ and storage are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import LocalStorage, get_extract_path, get_transform_path, DEFAULT_STORAGE_BASE, load_storage_config, create_component_backend
from storage.run_logging import install_run_logging, resolve_log_dir
from transformer.pipeline import run_pipeline
from transformer.logging_utils import configure_logging, create_operation_id, log_operation

logger = logging.getLogger("transformer.cli")


def main():
    parser = argparse.ArgumentParser(
        description="Transform raw JSON logs to Parquet using a mapping config."
    )
    parser.add_argument(
        "--storage-path",
        default=DEFAULT_STORAGE_BASE,
        help=f"Storage base path (default: {DEFAULT_STORAGE_BASE}). Reads from {{path}}/extract, writes to {{path}}/transform. Use one base per tenant.",
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
        help="Path to storage config YAML. When set, overrides --storage-path for backend selection.",
    )
    parser.add_argument(
        "--mapping",
        action="append",
        dest="mappings",
        required=True,
        metavar="MAPPING_YAML",
        help=(
            "Path to a mapping YAML file. Repeat this flag to process multiple mappings "
            "in one invocation (e.g. --mapping event_log.yaml --mapping snapshots.yaml). "
            "Each mapping produces its own output file(s)."
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Override output directory (default: {storage-path}/transform)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO).",
    )
    args = parser.parse_args()

    log_dir_abs = resolve_log_dir(args.storage_path, args.log_dir)
    install_run_logging("transformer", log_dir_abs, args.log_retention_days)

    level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    configure_logging(level=level)

    storage_config = None
    if args.storage_config:
        storage_config = load_storage_config(args.storage_config)

    if storage_config:
        storage_raw = create_component_backend(storage_config, storage_path=args.storage_path, component="extract")
        extract_path = storage_raw.base_path
    else:
        extract_path = get_extract_path(args.storage_path)
        storage_raw = LocalStorage(extract_path)

    output_dir = args.output or get_transform_path(args.storage_path)

    # Resolve and validate all mapping paths.
    mapping_paths = []
    for raw_path in args.mappings:
        mp = raw_path if os.path.isabs(raw_path) else os.path.join(os.getcwd(), raw_path)
        if not os.path.isfile(mp):
            logger.error("Mapping file not found: %s", mp)
            sys.exit(1)
        mapping_paths.append(mp)

    op_id = create_operation_id()
    log_operation(
        op_id,
        "transformer_cli",
        "start",
        inputs={
            "extract_path": extract_path if storage_config else os.path.abspath(extract_path),
            "output_dir": os.path.abspath(output_dir),
            "mapping_paths": mapping_paths,
            "mapping_count": len(mapping_paths),
        },
    )

    all_written = []
    for mapping_path in mapping_paths:
        logger.info("Running mapping: %s", mapping_path)
        written, output_format = run_pipeline(storage_raw, output_dir, mapping_path)
        all_written.extend(written)

    log_operation(
        op_id,
        "transformer_cli",
        "complete",
        result={
            "status": "success",
            "mapping_count": len(mapping_paths),
            "written_count": len(all_written),
            "written_paths": all_written,
        },
    )
    logger.info("Transformer complete: %d mapping(s) processed, %d file(s) written.", len(mapping_paths), len(all_written))
    for p in all_written:
        logger.info("  %s", p)


if __name__ == "__main__":
    main()
