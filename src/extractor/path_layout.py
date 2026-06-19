"""
Map Platform Data API paths (date-first) to local extract paths.

The API returns date-first paths of the form:
  .../y=.../m=.../d=.../h=.../{segment_key}={value}/...

For activity logs, {segment_key} is "eventType".
For entity snapshots, {segment_key} is "entityType".

The optional local layout schema-first moves the {segment_key}= segment
before the y=/m=/d=/h= block.  All functions accept a ``segment_key``
parameter (default ``"eventType"`` for backward compatibility).

Existing callers that do not pass ``segment_key`` continue to work unchanged.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import List, Optional, Tuple

# Default partition key – keeps all existing callers working without changes.
_DEFAULT_KEY = "eventType"


class ExtractPathLayout(str, Enum):
    """Local storage path layout under extract/."""

    DATE_FIRST = "date_first"    # mirroring API: y/m/d/h then {key}=
    SCHEMA_FIRST = "schema_first"  # {key}=, then y/m/d/h

    @classmethod
    def from_cli(cls, s: str) -> "ExtractPathLayout":
        v = (s or "").strip().lower().replace("-", "_")
        for member in cls:
            if member.value == v or member.name.lower() == v:
                return member
        raise ValueError(
            f"Invalid extract path layout: {s!r}. Use 'date-first' or 'schema-first'."
        )


def normalize_slashes(path: str) -> str:
    if not path:
        return path
    return path.replace("\\", "/").rstrip("/")


def _find_segment_index(parts: List[str], prefix: str) -> int:
    for i, p in enumerate(parts):
        if p.startswith(prefix):
            return i
    return -1


def _parse_date_first_segments(
    parts: List[str],
    segment_key: str = _DEFAULT_KEY,
) -> Optional[Tuple[List[str], List[str], str, List[str]]]:
    """
    Parse a date-first path into:
      (prefix before y=, [y, m, d, h] segments, {segment_key}= segment, remainder)
    """
    y_idx = _find_segment_index(parts, "y=")
    if y_idx < 0:
        return None
    if y_idx + 3 >= len(parts):
        return None
    m_seg, d_seg, h_seg = parts[y_idx + 1 : y_idx + 4]
    if not m_seg.startswith("m=") or not d_seg.startswith("d=") or not h_seg.startswith("h="):
        return None
    if y_idx + 4 >= len(parts):
        return None
    key_seg = parts[y_idx + 4]
    if not key_seg.startswith(f"{segment_key}="):
        return None
    prefix = parts[:y_idx]
    date_block = parts[y_idx : y_idx + 4]
    remainder = parts[y_idx + 5 :]
    return (prefix, date_block, key_seg, remainder)


def map_api_to_local_relpath(
    api_path: str,
    layout: ExtractPathLayout,
    segment_key: str = _DEFAULT_KEY,
) -> str:
    """
    Return path relative to extract/ for storing a file.

    ``api_path`` is the value from the API (always date-first).
    ``segment_key`` is the partition key in the path (default "eventType";
    pass "entityType" for snapshot paths).
    """
    n = normalize_slashes(api_path)
    if not n:
        return n
    parts = [p for p in n.split("/") if p]
    if layout == ExtractPathLayout.DATE_FIRST:
        return n
    if layout == ExtractPathLayout.SCHEMA_FIRST:
        parsed = _parse_date_first_segments(parts, segment_key)
        if not parsed:
            raise ValueError(
                f"Cannot map path to schema-first layout "
                f"(expected .../y=.../m=.../d=.../h=.../{segment_key}=.../...): {api_path!r}"
            )
        prefix, date_block, key_seg, remainder = parsed
        # schema-first: .../prefix/{key}=.../y/m/d/h/.../remainder
        out_parts = prefix + [key_seg] + date_block + remainder
        return "/".join(out_parts)
    raise ValueError(f"Unknown layout: {layout}")


def other_layout(layout: ExtractPathLayout) -> ExtractPathLayout:
    if layout == ExtractPathLayout.DATE_FIRST:
        return ExtractPathLayout.SCHEMA_FIRST
    return ExtractPathLayout.DATE_FIRST


def _classify_path_if_known(
    rel_path: str,
    segment_key: str = _DEFAULT_KEY,
) -> Optional[ExtractPathLayout]:
    """If rel_path has both y= and {segment_key}=, return which layout the ordering matches."""
    parts = [p for p in rel_path.replace("\\", "/").split("/") if p]
    y_idx = _find_segment_index(parts, "y=")
    key_idx = _find_segment_index(parts, f"{segment_key}=")
    if y_idx < 0 or key_idx < 0:
        return None
    if y_idx < key_idx:
        return ExtractPathLayout.DATE_FIRST
    return ExtractPathLayout.SCHEMA_FIRST


def detect_layout_from_extract_root(
    extract_root: str,
    segment_key: str = _DEFAULT_KEY,
) -> Optional[ExtractPathLayout]:
    """
    Return date_first or schema_first if the tree has clear, consistent evidence; else None.
    """
    if not os.path.isdir(extract_root):
        return None
    seen: Optional[ExtractPathLayout] = None
    for dirpath, _, filenames in os.walk(extract_root):
        for name in filenames:
            if name.startswith("."):
                continue
            rel = os.path.relpath(os.path.join(dirpath, name), extract_root)
            c = _classify_path_if_known(rel.replace(os.sep, "/"), segment_key)
            if c is None:
                continue
            if seen is None:
                seen = c
            elif seen != c:
                return None  # ambiguous / mixed
    return seen


def assert_layout_matches_disk(
    chosen: ExtractPathLayout,
    extract_root: str,
    segment_key: str = _DEFAULT_KEY,
) -> None:
    """
    If any file path clearly classifies as the other layout, raise ValueError.
    If empty or no classifiable path, do not raise.
    """
    if not os.path.isdir(extract_root):
        return
    for dirpath, _, filenames in os.walk(extract_root):
        for name in filenames:
            if name.startswith("."):
                continue
            rel = os.path.relpath(os.path.join(dirpath, name), extract_root)
            c = _classify_path_if_known(rel.replace(os.sep, "/"), segment_key)
            if c is not None and c != chosen:
                raise ValueError(
                    f"Layout mismatch: you chose {chosen.value}, but '{extract_root}' already "
                    f"contains files in the {c.value} layout "
                    f"(example: {rel.replace(os.sep, '/')}). "
                    "Use a new --storage-path, clear the extract/ folder, or set "
                    "--extract-path-layout to match existing data."
                )


def detect_layout_from_storage(
    storage_backend,
    segment_key: str = _DEFAULT_KEY,
) -> Optional[ExtractPathLayout]:
    """Like :func:`detect_layout_from_extract_root` but works with any StorageBackend."""
    all_files = storage_backend.list_files()
    seen: Optional[ExtractPathLayout] = None
    for rel_path in all_files:
        if os.path.basename(rel_path).startswith("."):
            continue
        c = _classify_path_if_known(rel_path.replace("\\", "/"), segment_key)
        if c is None:
            continue
        if seen is None:
            seen = c
        elif seen != c:
            return None
    return seen


def assert_layout_matches_storage(
    chosen: ExtractPathLayout,
    storage_backend,
    segment_key: str = _DEFAULT_KEY,
) -> None:
    """Like :func:`assert_layout_matches_disk` but works with any StorageBackend."""
    all_files = storage_backend.list_files()
    for rel_path in all_files:
        if os.path.basename(rel_path).startswith("."):
            continue
        c = _classify_path_if_known(rel_path.replace("\\", "/"), segment_key)
        if c is not None and c != chosen:
            raise ValueError(
                f"Layout mismatch: you chose {chosen.value}, but storage already contains files "
                f"in the {c.value} layout (example: {rel_path.replace(os.sep, '/')}). "
                "Use a new --storage-path, clear the extract/ folder, or set "
                "--extract-path-layout to match existing data."
            )


def file_present_for_layouts(
    base_path: str,
    api_path: str,
    primary: ExtractPathLayout,
    check_alternate: bool,
    segment_key: str = _DEFAULT_KEY,
) -> bool:
    """
    True if the file already exists at the primary local path, or at the alternate layout
    (same API path) when check_alternate is True and mapping succeeds.
    """
    p1 = map_api_to_local_relpath(api_path, primary, segment_key)
    p1f = os.path.join(base_path, p1)
    if os.path.exists(p1f):
        return True
    if not check_alternate:
        return False
    other = other_layout(primary)
    try:
        p2 = map_api_to_local_relpath(api_path, other, segment_key)
    except ValueError:
        return False
    return os.path.exists(os.path.join(base_path, p2))
