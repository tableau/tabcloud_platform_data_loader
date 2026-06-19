"""
Dataset profiles for Platform Data API resources.

A DatasetProfile captures the API-level differences between the two data
retrieval capabilities exposed by the TCM Platform Data API:

  - ACTIVITYLOG: append-only site/tenant event stream (/activitylog endpoint)
  - SNAPSHOTS:   point-in-time entity state dumps (/snapshots endpoint)

Both share the same mechanical pattern (list → presign → download NDJSON/Parquet)
but differ in their URL resource name, query-filter parameter, and path-segment key.

Threading a DatasetProfile through LogRetriever, path_layout, and presence means
the strings "activitylog" and "eventType" are never hardcoded in logic — only
referenced through the profile.

Backward compatibility: all existing code that does not supply a dataset_profile
receives ACTIVITYLOG implicitly and behaves exactly as before.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetProfile:
    """Describes how to interact with one Platform Data API resource."""

    name: str
    """Human-readable name used in log messages (e.g. "activitylog", "snapshots")."""

    resource: str
    """URL path segment replacing /activitylog in API calls (e.g. "activitylog", "snapshots")."""

    filter_param: str
    """API query-parameter name for per-type filtering (e.g. "eventType", "entityType")."""

    path_key: str
    """Key used in the extract/ path segment (e.g. "eventType", "entityType")."""

    is_site_only: bool = False
    """
    True when the API only supports site-scoped calls (not tenant-scoped).
    LogRetriever will skip tenant calls and emit an informational message instead.
    """

    files_are_parquet: bool = False
    """
    True when the downloaded files are already Parquet (not NDJSON).
    The transformer uses passthrough copy mode instead of JSON parsing.
    """


# ---------------------------------------------------------------------------
# Canonical profiles
# ---------------------------------------------------------------------------

ACTIVITYLOG = DatasetProfile(
    name="activitylog",
    resource="activitylog",
    filter_param="eventType",
    path_key="eventType",
    is_site_only=False,
    files_are_parquet=False,
)
"""
Activity-log event stream.
Available to all editions (tiered by frequency/retention).
Supports both site-scoped and tenant-scoped calls.
Files are plain-text NDJSON.
"""

SNAPSHOTS = DatasetProfile(
    name="snapshots",
    resource="snapshots",
    filter_param="entityType",
    path_key="entityType",
    is_site_only=True,   # Tenant-level snapshots not yet available at launch (expected ~2027+)
    files_are_parquet=True,
)
"""
Entity snapshot dumps (Users, Groups, Workbooks, etc.).
Enterprise / Tableau+ editions only at launch.
Site-scoped only at launch; tenant-scoped is planned but not yet available.
Files are already in Parquet format — no NDJSON parsing needed.
"""

# Registry for lookup by name (e.g. from CLI --dataset flag)
_PROFILES: dict[str, DatasetProfile] = {
    ACTIVITYLOG.name: ACTIVITYLOG,
    SNAPSHOTS.name: SNAPSHOTS,
}


def get_profile(name: str) -> DatasetProfile:
    """Return the DatasetProfile for the given name (case-insensitive).

    :param name: Profile name ("activitylog" or "snapshots").
    :raises ValueError: If the name is not recognised.
    """
    key = (name or "").strip().lower()
    profile = _PROFILES.get(key)
    if profile is None:
        raise ValueError(
            f"Unknown dataset profile: {name!r}. Valid values: {sorted(_PROFILES)}."
        )
    return profile
