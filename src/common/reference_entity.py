"""
Reference entity descriptors for tenant-wide TCM REST API data.

A ReferenceEntity captures the conceptual differences between the two
tenant-wide reference datasets pulled directly from the TCM REST API
(not from the Platform Data API presign/download flow):

  - tenant_sites: one row per tenant site — site attributes from
                  GET /api/v1/tenants/{tenantId}/sites.
  - tenant_users: membership grain, one row per (userLuid x siteLuid) —
                  user attributes from GET /api/v1/tenants/{tenantId}/users,
                  expanded from each user's siteRoles[] array.

Unlike DatasetProfile (which drives list→presign→download Parquet), reference
entities use a direct paginated GET → write raw JSON pages → transformer-normalize
pipeline. The loader side is identical to snapshot tables (write_mode: snapshot,
drop+recreate on new pull).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReferenceEntity:
    """Describes one tenant-wide reference dataset pulled from the TCM REST API."""

    name: str
    """Internal name used in logs and path segments (e.g. "tenant_sites")."""

    table: str
    """Hyper table name (e.g. "tenant_sites", "tenant_users")."""

    grain: str
    """
    Row grain description:
      "identity"   — one row per unique entity (e.g. one row per site).
      "membership" — one row per entity x membership (e.g. one row per user x site).
    """

    api_path: str
    """
    TCM REST endpoint path template (without base URL), e.g.
    "/api/v1/tenants/{tenant_id}/users".
    """

    items_key: str
    """Top-level JSON key whose value is the list of items, e.g. "users" or "sites"."""

    description: str = ""
    """Human-readable description for documentation."""


# ---------------------------------------------------------------------------
# Canonical reference entities
# ---------------------------------------------------------------------------

TENANT_SITES = ReferenceEntity(
    name="tenant_sites",
    table="tenant_sites",
    grain="identity",
    api_path="/api/v1/tenants/{tenant_id}/sites",
    items_key="sites",
    description=(
        "All sites in the tenant. One row per site. "
        "Columns: siteLuid (alias of siteUUID), contentUrl, status, suspendedDateTime. "
        "Join key to ActivityLog: siteLuid."
    ),
)
"""
Tenant-wide site reference list.
Source: GET /api/v1/tenants/{tenantId}/sites (paginated).
"""

TENANT_USERS = ReferenceEntity(
    name="tenant_users",
    table="tenant_users",
    grain="membership",
    api_path="/api/v1/tenants/{tenant_id}/users",
    items_key="users",
    description=(
        "All users in the tenant, expanded by site membership. "
        "One row per (userLuid x siteLuid). "
        "Columns: userLuid (alias of userId), siteLuid (alias of siteId from siteRoles[]), "
        "siteRole, idpType, plus all user-level attributes. "
        "Users with no siteRoles emit one row with null siteLuid. "
        "Join keys to ActivityLog: userLuid (+ siteLuid for membership context)."
    ),
)
"""
Tenant-wide user reference list, membership grain.
Source: GET /api/v1/tenants/{tenantId}/users (paginated, returns siteRoles[] inline).
"""

# Registry for lookup by name (e.g. from CLI --reference-entities flag)
_REFERENCE_ENTITIES: dict[str, ReferenceEntity] = {
    TENANT_SITES.name: TENANT_SITES,
    TENANT_USERS.name: TENANT_USERS,
}


def get_reference_entity(name: str) -> ReferenceEntity:
    """Return the ReferenceEntity for the given name (case-insensitive).

    :param name: Entity name ("tenant_sites" or "tenant_users").
    :raises ValueError: If the name is not recognised.
    """
    key = (name or "").strip().lower()
    entity = _REFERENCE_ENTITIES.get(key)
    if entity is None:
        raise ValueError(
            f"Unknown reference entity: {name!r}. "
            f"Valid values: {sorted(_REFERENCE_ENTITIES)}."
        )
    return entity


def parse_reference_entities(value: str) -> list[ReferenceEntity]:
    """Parse a comma-separated reference entity name string into validated entities.

    :param value: e.g. "tenant_sites,tenant_users" or "" (empty = none).
    :returns: List of ReferenceEntity objects (empty list if value is blank).
    :raises ValueError: If any name is not recognised.
    """
    if not value or not value.strip():
        return []
    names = [n.strip() for n in value.split(",") if n.strip()]
    return [get_reference_entity(n) for n in names]
