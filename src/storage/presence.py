"""
Batch presence checking for S3 storage backends.

Builds a set of existing keys using ListObjectsV2 with date-anchored prefixes
rather than per-file HeadObject. For local backends, falls back to individual
exists() calls.
"""

import concurrent.futures
import re
from typing import List, Optional, Set, Tuple

from storage.base import StorageBackend


def _parse_date_segments(path: str) -> Optional[Tuple[str, str, str]]:
    """Extract (y, m, d) values from a path containing y=.../m=.../d=... segments."""
    m_y = re.search(r"y=(\d+)", path)
    m_m = re.search(r"m=(\d+)", path)
    m_d = re.search(r"d=(\d+)", path)
    if m_y and m_m and m_d:
        return (m_y.group(1), m_m.group(1), m_d.group(1))
    return None


def _parse_partition_value(path: str, key: str = "eventType") -> Optional[str]:
    """Extract a ``key=value`` segment value from *path*.

    Works for any partition key (``eventType``, ``entityType``, etc.).
    """
    needle = f"{key}="
    idx = path.find(needle)
    if idx < 0:
        return None
    start = idx + len(needle)
    end = path.find("/", start)
    if end < 0:
        return path[start:] or None
    return path[start:end] or None


# Back-compat alias used by older callers.
def _parse_event_type(path: str) -> Optional[str]:
    """Extract eventType value from a path. Kept for backward compatibility."""
    return _parse_partition_value(path, "eventType")


def build_presence_set(
    backend: StorageBackend,
    candidate_paths: List[str],
    layout: str,
    max_threads: int = 10,
    partition_key: str = "eventType",
) -> Set[str]:
    """Build a set of existing relative paths by batch-listing storage.

    For local backends, just does individual exists() checks (fast on local FS).
    For S3 backends, uses date-anchored prefix listing.

    :param backend: The StorageBackend to query.
    :param candidate_paths: List of relative paths (already mapped to storage layout).
    :param layout: "date_first" or "schema_first".
    :param max_threads: Thread pool size for parallelizing S3 list calls.
    :param partition_key: Path-segment key to use for schema-first prefix grouping
        (default "eventType"; pass "entityType" for snapshot paths).
    :return: Set of relative paths that exist in storage.
    """
    from storage.local import LocalStorage
    if isinstance(backend, LocalStorage):
        return _presence_set_local(backend, candidate_paths)

    return _presence_set_remote(backend, candidate_paths, layout, max_threads, partition_key)


def _presence_set_local(backend: StorageBackend, candidate_paths: List[str]) -> Set[str]:
    """Fast path for local storage: individual exists() calls."""
    return {p for p in candidate_paths if backend.exists(p)}


def _presence_set_remote(
    backend: StorageBackend,
    candidate_paths: List[str],
    layout: str,
    max_threads: int,
    partition_key: str = "eventType",
) -> Set[str]:
    """Build presence set using batch ListObjectsV2 with date-anchored prefixes.

    Groups candidates by date (and partition_key for schema-first), lists each
    prefix group, and merges results into a single set.
    """
    if not candidate_paths:
        return set()

    prefixes = _compute_list_prefixes(candidate_paths, layout, partition_key)

    if not prefixes:
        all_files = backend.list_files()
        return set(all_files)

    all_keys: Set[str] = set()
    if len(prefixes) == 1:
        keys = backend.list_files(prefix=prefixes[0])
        all_keys.update(keys)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_threads, len(prefixes))) as pool:
            futures = {pool.submit(backend.list_files, prefix=p): p for p in prefixes}
            for future in concurrent.futures.as_completed(futures):
                try:
                    all_keys.update(future.result())
                except Exception:
                    pass  # prefix may not exist

    return all_keys


def _compute_list_prefixes(
    candidate_paths: List[str],
    layout: str,
    partition_key: str = "eventType",
) -> List[str]:
    """Compute the set of S3 prefixes to list for presence checking.

    Never broader than one calendar month.
    For date-first: y=Y/m=M/d=D/ for single-day, y=Y/m=M/ for multi-day.
    For schema-first: {partition_key}=E/y=Y/m=M/d=D/ for single-day,
    {partition_key}=E/y=Y/m=M/ for multi-day.

    :param partition_key: The path segment key to group by for schema-first layout
        (typically "eventType" for activity logs, "entityType" for snapshots).
    """
    date_tuples: Set[Tuple[str, str, str]] = set()
    month_tuples: Set[Tuple[str, str]] = set()
    partition_values: Set[str] = set()

    for path in candidate_paths:
        parsed = _parse_date_segments(path)
        if parsed:
            y, m, d = parsed
            date_tuples.add((y, m, d))
            month_tuples.add((y, m))
        pv = _parse_partition_value(path, partition_key)
        if pv:
            partition_values.add(pv)

    if not date_tuples:
        return []

    use_day_level = len(date_tuples) <= 1

    prefixes = []

    if layout == "schema_first":
        if not partition_values:
            return []
        if use_day_level:
            y, m, d = next(iter(date_tuples))
            for pv in sorted(partition_values):
                prefixes.append(f"{partition_key}={pv}/y={y}/m={m}/d={d}/")
        else:
            for y, m in sorted(month_tuples):
                for pv in sorted(partition_values):
                    prefixes.append(f"{partition_key}={pv}/y={y}/m={m}/")
    else:
        if use_day_level:
            y, m, d = next(iter(date_tuples))
            prefixes.append(f"y={y}/m={m}/d={d}/")
        else:
            for y, m in sorted(month_tuples):
                prefixes.append(f"y={y}/m={m}/")

    return prefixes
