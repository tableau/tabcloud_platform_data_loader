"""
Load state: per-table last-max value (incremental) and snapshot timestamp (snapshot load).

Incremental load: last_max of the increment_field, stored under table name key.
Snapshot load: last loaded snapshot_time (ISO string), stored under "{table}_snapshot_time" key.

Both types share a single JSON sidecar next to the Hyper file:
  e.g. platform_data_bundle_load_state.json

  {
    "ActivityLog": "2026-06-07T15:00:00.000Z",           // last incremental max
    "Users_snapshot_time": "2026-06-08T14:00:00Z",       // last loaded snapshot time
    "Groups_snapshot_time": "2026-06-08T14:00:00Z",
    ...
  }
"""

import json
import os
from typing import Any, Optional

from loader.logging_utils import get_logger

logger = get_logger("load_state")


def _state_path(hyper_path: str) -> str:
    """Path to the load state JSON file for this Hyper file."""
    base = hyper_path or ""
    if not base:
        return ""
    dirname = os.path.dirname(base)
    name = os.path.basename(base)
    stem, _ = os.path.splitext(name)
    if not stem:
        stem = "load"
    return os.path.join(dirname or ".", f"{stem}_load_state.json")


def get_last_increment_max(hyper_path: str, table: str) -> Optional[Any]:
    """
    Read the last loaded max value for the increment field for the given table.
    Returns None if no state file or no value for this table.

    :param hyper_path: Path to the .hyper file (state file lives alongside it).
    :param table: Table name (e.g. Extract).
    :return: Last max value (e.g. timestamp string or number), or None.
    """
    path = _state_path(hyper_path)
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(table)
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def set_last_increment_max(hyper_path: str, table: str, value: Any) -> None:
    """
    Write the last loaded max value for the increment field for the given table.
    Merges with existing state for other tables.

    :param hyper_path: Path to the .hyper file.
    :param table: Table name.
    :param value: Max value to store (must be JSON-serializable: str, number, etc.).
    """
    _write_state_key(hyper_path, table, value)
    logger.debug("Wrote load state last_increment_max for %s: %s", table, value)


def _write_state_key(hyper_path: str, key: str, value: Any) -> None:
    """Write a single key-value pair into the load state sidecar, merging with existing state."""
    path = _state_path(hyper_path)
    if not path:
        return
    dirname = os.path.dirname(path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    data = {}
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError, TypeError):
            pass
    if not isinstance(data, dict):
        data = {}
    data[key] = value
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Snapshot watermark helpers
# ---------------------------------------------------------------------------

_SNAPSHOT_SUFFIX = "_snapshot_time"


def get_last_snapshot_time(hyper_path: str, table: str) -> Optional[str]:
    """
    Read the last successfully loaded snapshot timestamp for the given table.

    Stored under the key ``"{table}_snapshot_time"`` in the load state sidecar.
    Returns None if no watermark has been written yet (first run → always replace).

    :param hyper_path: Path to the .hyper file (state file lives alongside it).
    :param table: Table name (e.g. "Users").
    :return: ISO timestamp string (e.g. "2026-06-08T14:00:00Z"), or None.
    """
    path = _state_path(hyper_path)
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(f"{table}{_SNAPSHOT_SUFFIX}")
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def set_last_snapshot_time(hyper_path: str, table: str, snapshot_time: str) -> None:
    """
    Write the snapshot timestamp watermark for the given table after a successful replace.

    :param hyper_path: Path to the .hyper file.
    :param table: Table name (e.g. "Users").
    :param snapshot_time: ISO timestamp string (e.g. "2026-06-08T14:00:00Z").
    """
    _write_state_key(hyper_path, f"{table}{_SNAPSHOT_SUFFIX}", snapshot_time)
    logger.debug("Wrote snapshot watermark for %s: %s", table, snapshot_time)


# ---------------------------------------------------------------------------
# Per-destination publish watermark (published_through)
# ---------------------------------------------------------------------------

_PUBLISHED_THROUGH_PREFIX = "published_through__"


def get_published_through(hyper_path: str, destination_name: str) -> Optional[str]:
    """
    Return the ISO timestamp of the last *successful* Tableau publish for this
    destination, or None if no publish has ever succeeded.

    Stored under ``"published_through__{destination_name}"`` in the load state
    sidecar alongside the .hyper file.

    A missing/None marker means: publish on the next load run, even if no new
    data was written locally.  This ensures a failed publish is retried on the
    next run without any manual intervention.

    :param hyper_path: Path to the .hyper file.
    :param destination_name: The destination name from run.yml destinations map.
    :return: ISO timestamp string or None.
    """
    path = _state_path(hyper_path)
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(f"{_PUBLISHED_THROUGH_PREFIX}{destination_name}")
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def set_published_through(hyper_path: str, destination_name: str, value: str) -> None:
    """
    Write the publish watermark for a destination after a *successful* publish.

    Advance this value ONLY after the publish fully succeeds — never on failure.
    This ensures a failed publish is automatically retried on the next run.

    :param hyper_path: Path to the .hyper file.
    :param destination_name: The destination name from run.yml destinations map.
    :param value: ISO timestamp string (e.g. "2026-06-08T14:00:00Z").
    """
    _write_state_key(hyper_path, f"{_PUBLISHED_THROUGH_PREFIX}{destination_name}", value)
    logger.debug("Wrote published_through for destination '%s': %s", destination_name, value)
