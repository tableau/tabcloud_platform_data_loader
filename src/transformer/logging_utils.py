"""
Structured JSON logging for the transformer (same format as extractor log_operation).
"""

import datetime
import json
import logging
import uuid


def get_current_time_iso():
    """Returns the current UTC time in ISO 8601 format."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def create_operation_id():
    """Generates a unique ID for an operation."""
    return str(uuid.uuid4())


def log_operation(operation_id, operation_name, state, inputs=None, result=None, duration=None):
    """
    Logs an operation in a standardized JSON format (matches extractor).

    :param operation_id: A unique ID for the operation.
    :param operation_name: The name of the operation.
    :param state: The state of the operation ('start', 'processing', 'complete').
    :param inputs: A dictionary of key inputs to the operation.
    :param result: A dictionary with the operation result and details.
    :param duration: The duration of the operation in milliseconds.
    """
    log_entry = {
        "operationId": operation_id,
        "operationName": operation_name,
        "startDatetime": get_current_time_iso(),
        "state": state,
    }
    if inputs:
        log_entry["keyInputs"] = inputs
    if state in ("complete", "warning"):
        log_entry["endDatetime"] = get_current_time_iso()
        log_entry["result"] = result
        if duration is not None:
            log_entry["durationMs"] = duration

    # Structured operation entries are diagnostic detail; keep them at DEBUG so
    # INFO-level console output remains concise.
    logging.getLogger("transformer.operation").debug(json.dumps(log_entry, indent=2))


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging for transformer: stdout with timestamp and level.

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
