"""
Discover and read JSON / Parquet files from extract storage.

Paths encode a partition-key segment (e.g. ``eventType=Login`` or ``entityType=Users``).
Transformer mappings use ``folder_pattern`` and ``file_pattern``; bare segment patterns
(e.g. ``entityType=Users``) match at any depth.  Do not hand-edit extract/ into an
unsupported shape.

New in snapshot support:
  - ``select_files_for_mapping(files, config)`` applies file-selection modes:
      "all"                  — return files unchanged (default for events)
      "latest_per_partition" — keep only the newest file per partition-key value
      "all_snapshots"        — RESERVED for future SCD (not yet implemented)
"""

import fnmatch
import json
import os
import re
import sys
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage import (  # noqa: E402
    extract_event_type_from_path,
    extract_segment_value_from_path,
    is_valid_parquet_file,
)


def _folder_pattern_matches_path(folder_pattern: str, path: str, exact_only: bool = False) -> bool:
    """
    Return True if path matches folder_pattern.

    Wildcard semantics:
      * matches exactly one path segment (any characters except /).
      ** (globstar) matches zero or more path segments.

    If exact_only is False (default), path matches if it equals the pattern or is nested
    anywhere under a matching directory.
    If exact_only is True, path must exactly match the pattern with no extra trailing segments.

    Examples:
      "**/eventType=Login"            matches eventType=Login at any depth
      "pod=*/siteLuid=*/**/eventType=Login"  matches Login under any pod/site at any depth
      "*"                             matches everything
    """
    path_norm = path.replace("\\", "/")
    pattern_norm = folder_pattern.strip().rstrip("/") if folder_pattern else ""
    if not pattern_norm or pattern_norm == "*":
        return True

    parts = pattern_norm.split("/")
    regex_pieces = []
    for i, p in enumerate(parts):
        is_last = (i == len(parts) - 1)
        if p == "**":
            # Zero or more segments, each consuming its trailing slash.
            # The trailing slash is absorbed into the ** token, so no extra /
            # separator is appended after it.
            regex_pieces.append(r"([^/]+/)*")
        elif p == "*":
            regex_pieces.append(r"[^/]+")
            if not is_last:
                regex_pieces.append(r"/")
        else:
            regex_pieces.append(re.escape(p))
            if not is_last:
                regex_pieces.append(r"/")

    if exact_only:
        base_re = "^" + "".join(regex_pieces) + "$"
    else:
        base_re = "^" + "".join(regex_pieces) + r"(/.*)?$"
    return bool(re.match(base_re, path_norm))



def discover_json_files(storage, event_types=None, prefix=""):
    """
    List JSON files in storage under prefix. If event_types is given, filter to paths
    whose eventType= segment is in that set.

    :param storage: StorageBackend-like (list_files(prefix), full_path(rel))
    :param event_types: Optional list of event type names to include
    :param prefix: Optional prefix under base_path
    :return: List of relative paths to JSON files
    """
    all_files = storage.list_files(prefix=prefix)
    json_files = [p for p in all_files if p.endswith(".json")]
    if not event_types:
        return json_files
    allowed = set(event_types)
    return [p for p in json_files if extract_event_type_from_path(p) in allowed]


def discover_files_by_source(storage, folder_pattern: str, file_pattern: str, recursive: bool = True):
    """
    List files in storage under paths matching folder_pattern (within storage base),
    with filename matching file_pattern.

    folder_pattern wildcard semantics:
      *   matches exactly one path segment (no slashes).
      **  (globstar) matches zero or more path segments at any depth.
      ""  or "*" matches the entire tree.

    Bare single-segment patterns (no slashes, no wildcards, e.g. "eventType=Login") are
    automatically treated as "**/eventType=Login", matching that directory at any depth
    in the tree. This avoids the footgun where the extract layout nests event-type
    directories several levels deep and a literal prefix would find nothing.

    Multi-segment literals (e.g. "pod=10az/siteLuid=abc") are used as a path prefix
    from the storage root, same as before.

    :param storage: StorageBackend-like with list_files(prefix)
    :param folder_pattern: Glob-style path pattern (* = one segment, ** = any depth),
                           "" / "*" for all files, or a bare segment name.
    :param file_pattern: Glob pattern for filename (e.g. "*.json", "*.json")
    :param recursive: If True, include files in subdirs of matched folder; if False,
                      only direct children of the matching folder.
    :return: List of relative paths to matching files
    """
    pattern = (folder_pattern or "").strip().rstrip("/")
    has_any_wildcard = "*" in pattern  # catches both * and **

    # Bare single-segment literal (no slashes, no wildcards): auto-prepend **/
    # so "eventType=Login" finds Login directories at any depth in the tree.
    is_bare_segment = bool(pattern) and "/" not in pattern and not has_any_wildcard
    if is_bare_segment:
        effective_pattern = f"**/{pattern}"
    else:
        effective_pattern = pattern

    # Determine the os.walk starting prefix:
    # - Full tree walk when: ** is involved (any depth possible), bare segment rewrites,
    #   or single-star wildcards (can't safely compute a concrete prefix).
    # - Concrete prefix when: multi-segment literal with no wildcards (user knows exact path).
    has_wildcard = "*" in effective_pattern
    if has_wildcard or is_bare_segment or not effective_pattern or effective_pattern == "*":
        prefix = ""
    else:
        prefix = effective_pattern  # multi-segment literal: use as os.walk root

    all_files = storage.list_files(prefix=prefix)
    result = []
    for rel_path in all_files:
        path_norm = rel_path.replace("\\", "/")
        if effective_pattern and effective_pattern != "*":
            if not _folder_pattern_matches_path(effective_pattern, path_norm):
                continue
        name = os.path.basename(rel_path)
        if not fnmatch.fnmatch(name, file_pattern):
            continue
        if not recursive and effective_pattern and effective_pattern != "*":
            dir_part = os.path.dirname(path_norm)
            if not _folder_pattern_matches_path(effective_pattern, dir_part, exact_only=True):
                continue
        result.append(rel_path)
    return result


def _path_to_date_tuple(path: str):
    """Derive a sortable (year, month, day, hour) tuple from a path's y=/m=/d=/h= segments.

    Segments must appear as standalone path components (preceded by '/' or at the start of the
    string) to avoid matching key characters inside other segment names such as 'pod=' or
    'siteLuid='.

    Returns (0, 0, 0, 0) if date segments are missing (so undated files sort lowest).
    """
    def _seg(key: str):
        # Require the key to be a proper path segment: either at the very start of
        # the path or immediately after a '/'.
        slash_needle = f"/{key}="
        idx = path.find(slash_needle)
        if idx >= 0:
            start = idx + len(slash_needle)
        elif path.startswith(f"{key}="):
            start = len(key) + 1
        else:
            return None
        end = path.find("/", start)
        return path[start:] if end < 0 else path[start:end]

    try:
        y = int(_seg("y") or 0)
        m = int(_seg("m") or 0)
        d = int(_seg("d") or 0)
        h = int(_seg("h") or 0)
        return (y, m, d, h)
    except (TypeError, ValueError):
        return (0, 0, 0, 0)


def select_latest_per_partition(files: List[str], partition_key: str = "eventType") -> List[str]:
    """
    Given a list of file paths, keep only the file(s) that belong to the *newest* snapshot
    for each partition-key value (e.g. newest 'entityType=Users' file).

    Files are grouped by the value of ``partition_key`` in their path (using the y=/m=/d=/h=
    date block for sorting within each group).  Files whose partition value cannot be
    determined are preserved unchanged.

    This is correct in both extractor modes:
      - In latest-only extract mode: no-op (only one snapshot per type is on disk).
      - In retain-all mode: actively selects the newest before union/copy.

    :param files: Relative file paths from storage.list_files().
    :param partition_key: Path segment key to group by (default "eventType"; use "entityType"
        for snapshot files).
    :returns: Filtered list of file paths.
    """
    groups: dict = {}  # partition_value -> [(date_tuple, rel_path)]
    ungrouped: List[str] = []

    for rel_path in files:
        pv = extract_segment_value_from_path(rel_path, partition_key)
        if pv is None:
            ungrouped.append(rel_path)
            continue
        date_key = _path_to_date_tuple(rel_path)
        if pv not in groups:
            groups[pv] = []
        groups[pv].append((date_key, rel_path))

    result: List[str] = list(ungrouped)
    for pv, items in groups.items():
        best = max(item[0] for item in items)
        result.extend(p for (dk, p) in items if dk == best)
    return result


def group_files_by_partition_newest_first(
    files: List[str], partition_key: str = "eventType"
) -> dict:
    """
    Group *files* by their partition-key value, with candidates sorted newest-first
    within each group (using the y=/m=/d=/h= date block from the path).

    Unlike :func:`select_latest_per_partition`, this retains ALL candidates per
    partition value so callers can implement fallback logic (e.g. try newest; if
    invalid, try second-newest, etc.).

    Files with no determinable partition value are collected under the ``None`` key.

    :param files: Relative file paths from storage.list_files().
    :param partition_key: Path segment key to group by (default "eventType").
    :returns: Dict mapping partition_value (or None) -> list of rel_paths, newest first.
    """
    groups: dict = {}
    for rel_path in files:
        pv = extract_segment_value_from_path(rel_path, partition_key)
        date_key = _path_to_date_tuple(rel_path)
        if pv not in groups:
            groups[pv] = []
        groups[pv].append((date_key, rel_path))
    return {
        pv: [p for _, p in sorted(items, key=lambda x: x[0], reverse=True)]
        for pv, items in groups.items()
    }


def select_latest_per_site_per_partition(
    files: List[str],
    partition_key: str = "entityType",
    site_key: str = "siteLuid",
) -> dict:
    """
    For each partition value (e.g. entityType), pick the newest snapshot per site
    and return ALL part files belonging to that newest hour.

    Algorithm:
      1. Group files by partition value (entityType).
      2. Within each partition group, further group by site_key (siteLuid).
         Files with no site_key segment are collected under ``None`` as a single group.
      3. Per site, determine the newest (y, m, d, h) date tuple and collect every
         file at exactly that date (all part-* files of that snapshot hour).
      4. Union the selected files across all sites.

    Returns a dict mapping each partition value to a tuple of:
      (selected_rel_paths: List[str], max_date_tuple: tuple)

    where max_date_tuple is the newest (y, m, d, h) seen across all sites for that
    entity (used for the manifest snapshot_time).

    :param files: Relative file paths from storage.list_files().
    :param partition_key: Path segment key to group entities by (default "entityType").
    :param site_key: Path segment key to group sites by (default "siteLuid").
    :returns: Dict mapping partition_value -> (rel_paths, max_date_tuple).
    """
    # Step 1: group by partition value.
    by_entity: dict = {}
    for rel_path in files:
        pv = extract_segment_value_from_path(rel_path, partition_key)
        if pv is None:
            continue
        by_entity.setdefault(pv, []).append(rel_path)

    result: dict = {}

    for pv, entity_files in by_entity.items():
        # Step 2: group by site within this entity.
        by_site: dict = {}
        for rel_path in entity_files:
            sv = extract_segment_value_from_path(rel_path, site_key)
            by_site.setdefault(sv, []).append(rel_path)

        selected: List[str] = []
        max_date: tuple = (0, 0, 0, 0)

        # Step 3: per site, find the newest date and collect all its parts.
        for sv, site_files in by_site.items():
            # Find the best (newest) date among this site's files.
            site_dates = [(_path_to_date_tuple(p), p) for p in site_files]
            if not site_dates:
                continue
            best_date = max(dt for dt, _ in site_dates)
            best_parts = [p for dt, p in site_dates if dt == best_date]
            selected.extend(best_parts)
            if best_date > max_date:
                max_date = best_date

        if selected:
            result[pv] = (selected, max_date)

    return result


def select_files_for_mapping(files: List[str], config: dict) -> List[str]:
    """
    Apply the ``select`` mode from a mapping config to a list of discovered files.

    Modes:
      "all" (default)          — return all files unchanged.
      "latest_per_partition"   — keep only the newest file per partition value
                                 (uses ``partition_field`` from config, default "eventType").
      "all_snapshots"          — RESERVED for future SCD / Type-2 history.
                                 Not yet implemented; falls back to "all" with a warning.
                                 Future extension point: union multiple snapshots of the same
                                 entity with effective-from/effective-to columns, controlled
                                 by a dedup-by-key and effective-dating step here.

    :param files: Discovered file paths.
    :param config: Mapping config dict (used to read ``select`` and ``partition_field``).
    :returns: Filtered (or unchanged) file list.
    """
    import logging
    _logger = logging.getLogger("transformer.reader")

    select_mode = str((config.get("select") or "all")).strip().lower()
    partition_field = str((config.get("partition_field") or "eventType")).strip()

    if select_mode == "all":
        return files

    if select_mode == "latest_per_partition":
        return select_latest_per_partition(files, partition_key=partition_field)

    if select_mode == "all_snapshots":
        # --- FUTURE SCD STUB ---
        # This mode is reserved for slowly-changing-dimension (Type 2) history.
        # When implemented it will:
        #   1. Union ALL available snapshots for each entity type (no filtering).
        #   2. Sort by snapshotTimestamp (from path date block).
        #   3. Assign effective_from / effective_to columns.
        #   4. Dedup by a configured business key + effective window.
        # The retain-all extractor mode (snapshot_selection=all) keeps every
        # historical snapshot on disk, providing the raw material for this.
        # See: entity_snapshot_auto.yaml commented "future use case" block.
        _logger.warning(
            "select=all_snapshots is reserved for future SCD / Type-2 history and is not "
            "yet implemented. Falling back to select=all (all discovered files included)."
        )
        return files

    _logger.warning("Unknown select mode %r; defaulting to 'all'.", select_mode)
    return files


def load_first_json_record(storage, relative_path):
    """
    Load the first non-empty JSON record from a file without reading the entire file.

    Uses storage.read_first_line() when available to avoid loading the full file into memory.
    Falls back to reading the full file if read_first_line is not supported.

    :param storage: StorageBackend-like with read_first_line(rel) or read_file(rel)/full_path(rel)
    :param relative_path: Path relative to storage base_path
    :return: A single dict, or None if the file is empty or unparseable
    """
    if hasattr(storage, "read_first_line"):
        line = storage.read_first_line(relative_path)
        if not line:
            return None
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None

    # Fallback: load all records and return the first
    records = load_json_records(storage, relative_path)
    return records[0] if records else None


def load_json_records(storage, relative_path):
    """
    Load newline-delimited JSON (NDJSON) from a file. Each non-empty line is one JSON object.

    :param storage: StorageBackend-like with read_file(relative_path) or full_path + open
    :param relative_path: Path relative to storage base_path
    :return: List of dicts (records)
    """
    if hasattr(storage, "read_file"):
        content = storage.read_file(relative_path)
    else:
        full_path = storage.full_path(relative_path)
        with open(full_path, "r", encoding="utf-8") as f:
            content = f.read()
    records = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records
