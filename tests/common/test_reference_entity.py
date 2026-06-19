"""Tests for the ReferenceEntity registry and helper functions."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from common.reference_entity import (
    get_reference_entity,
    parse_reference_entities,
    TENANT_SITES,
    TENANT_USERS,
    ReferenceEntity,
    _REFERENCE_ENTITIES,
)


class TestReferenceEntityRegistry:
    def test_known_entities_present(self):
        assert "tenant_sites" in _REFERENCE_ENTITIES
        assert "tenant_users" in _REFERENCE_ENTITIES

    def test_tenant_sites_attributes(self):
        assert TENANT_SITES.name == "tenant_sites"
        assert TENANT_SITES.table == "tenant_sites"
        assert TENANT_SITES.grain == "identity"
        assert "sites" in TENANT_SITES.api_path
        assert TENANT_SITES.items_key == "sites"

    def test_tenant_users_attributes(self):
        assert TENANT_USERS.name == "tenant_users"
        assert TENANT_USERS.table == "tenant_users"
        assert TENANT_USERS.grain == "membership"
        assert "users" in TENANT_USERS.api_path
        assert TENANT_USERS.items_key == "users"

    def test_reference_entity_is_frozen(self):
        with pytest.raises(Exception):
            TENANT_SITES.name = "changed"


class TestGetReferenceEntity:
    def test_lookup_tenant_sites(self):
        e = get_reference_entity("tenant_sites")
        assert e is TENANT_SITES

    def test_lookup_tenant_users(self):
        e = get_reference_entity("tenant_users")
        assert e is TENANT_USERS

    def test_case_insensitive(self):
        assert get_reference_entity("TENANT_SITES") is TENANT_SITES
        assert get_reference_entity("Tenant_Users") is TENANT_USERS

    def test_whitespace_stripped(self):
        assert get_reference_entity("  tenant_sites  ") is TENANT_SITES

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown reference entity"):
            get_reference_entity("unknown_entity")

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            get_reference_entity("")

    def test_none_raises(self):
        with pytest.raises(ValueError):
            get_reference_entity(None)


class TestParseReferenceEntities:
    def test_empty_string_returns_empty(self):
        assert parse_reference_entities("") == []

    def test_whitespace_returns_empty(self):
        assert parse_reference_entities("   ") == []

    def test_single_entity(self):
        result = parse_reference_entities("tenant_sites")
        assert result == [TENANT_SITES]

    def test_multiple_entities(self):
        result = parse_reference_entities("tenant_sites,tenant_users")
        assert result == [TENANT_SITES, TENANT_USERS]

    def test_whitespace_around_commas(self):
        result = parse_reference_entities(" tenant_sites , tenant_users ")
        assert result == [TENANT_SITES, TENANT_USERS]

    def test_unknown_entity_raises(self):
        with pytest.raises(ValueError, match="Unknown reference entity"):
            parse_reference_entities("tenant_sites,unknown_entity")
