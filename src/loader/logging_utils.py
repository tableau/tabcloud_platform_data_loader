"""
Logging for the loader. Uses standard library logging; optional structured JSON
for the main load operation (same format as extractor/transformer log_operation).
"""

import datetime
import json
import logging
import uuid
from typing import Optional


def get_current_time_iso() -> str:
    """Current UTC time in ISO 8601 format."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def create_operation_id() -> str:
    """Unique ID for an operation."""
    return str(uuid.uuid4())


def log_operation(
    operation_id: str,
    operation_name: str,
    state: str,
    inputs: Optional[dict] = None,
    result: Optional[dict] = None,
    duration: Optional[int] = None,
) -> None:
    """
    Log an operation in structured JSON (matches extractor/transformer).

    :param operation_id: Unique ID for the operation.
    :param operation_name: Name of the operation (e.g. load_hyper).
    :param state: 'start', 'processing', or 'complete'.
    :param inputs: Optional key inputs (storage_path, loader_mapping, etc.).
    :param result: Optional result dict (e.g. success, rows_written).
    :param duration: Optional duration in milliseconds.
    """
    log_entry = {
        "operationId": operation_id,
        "operationName": operation_name,
        "startDatetime": get_current_time_iso(),
        "state": state,
    }
    if inputs:
        log_entry["keyInputs"] = inputs
    if state == "complete":
        log_entry["endDatetime"] = get_current_time_iso()
        log_entry["result"] = result
        log_entry["durationMs"] = duration
    # Structured operation entries are diagnostic detail; keep them at DEBUG so
    # INFO-level console output remains concise.
    logging.getLogger("loader.operation").debug(json.dumps(log_entry, indent=2))


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging for loader: stdout with timestamp and level.

    Intended to run after :func:`storage.run_logging.install_run_logging` when using
    the CLI so StreamHandler attaches to the teed ``sys.stdout``.
    """
    import sys
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module (e.g. loader.loader)."""
    if not name.startswith("loader."):
        name = f"loader.{name}"
    return logging.getLogger(name)
