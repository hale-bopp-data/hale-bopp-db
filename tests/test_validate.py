"""Tests for hb test — 7 structural and security checks."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.compile import DataDictionary, load_dictionary
from app.core.validate import (
    ALL_CHECKS,
    SUITES,
    CheckSeverity,
    ValidationReport,
    get_suite_checks,
    validate_dictionary,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_dict(entities: list[dict], **overrides) -> DataDictionary:
    """Build a minimal DataDictionary for testing."""
    base = {
        "type_map": {
            "uuid": {"pg": "TEXT"},
            "string": {"pg": "TEXT"},
            "string(n)": {"pg": "VARCHAR({n})"},
            "integer": {"pg": "INTEGER"},
            "auto": {"pg": "BIGSERIAL"},
            "boolean": {"pg": "BOOLEAN"},
            "timestamp": {"pg": "TIMESTAMPTZ"},
            "date": {"pg": "DATE"},
            "decimal(p,s)": {"pg": "NUMERIC({p},{s})"},
            "json": {"pg": "JSONB"},
            "text_large": {"pg": "TEXT"},
            "long": {"pg": "BIGINT"},
        },
        "default_map": {
            "now()": {"pg": "NOW()"},
            "uuid()": {"pg": "gen_random_uuid()::TEXT"},
            "true": {"pg": "TRUE"},
            "false": {"pg": "FALSE"},
        },
        "name_mapping": [
            {"concept": "Organizzazione cliente", "canonical": "tenant", "aliases_forbidden": ["COD_CLI", "customer_id", "client_id"]},
            {"concept": "Email", "canonical": "email", "aliases_forbidden": ["EMAIL_ADDRESS", "user_email", "mail"]},
            {"concept": "Stato entità", "canonical": "status", "aliases_forbidden": ["STATO", "state", "flag_active"]},
        ],
        "entities": entities,
    }
    base.update(overrides)
    return DataDictionary.model_validate(base)


GOOD_ENTITY = {
    "name": "tenant",
    "schema": "platform",
    "type": "DIM",
    "description": "Root entity",
    "columns": [
        {"name": "id", "type": "auto", "nullable": False},
        {"name": "tenant_id", "type": "string(50)", "nullable": False},
        {"name": "tenant_name", "type": "string(255)", "nullable": False},
        {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
        {"name": "created_by", "type": "string", "nullable": False, "default": "'system'"},
        {"name": "updated_at", "type": "timestamp", "nullable": False, "default": "now()"},
        {"name": "updated_by", "type": "string", "nullable": False, "default": "'system'"},
    ],
}


# ---------------------------------------------------------------------------
# Check 1: canonical_name
# ---------------------------------------------------------------------------

class TestCanonicalName:
    def test_good_names_pass(self):
        dd = _make_dict([GOOD_ENTITY])
        report = validate_dictionary(dd, checks=[1])
        assert report.all_passed

    def test_forbidden_column_alias(self):
        entity = {**GOOD_ENTITY, "columns": [
            *GOOD_ENTITY["columns"],
            {"name": "COD_CLI", "type": "string(50)", "nullable": True},
        ]}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[1])
        assert report.failed > 0
        fail = next(r for r in report.results if not r.passed)
        assert "COD_CLI" in fail.message
        assert fail.fix == "Rename to 'tenant'"

    def test_forbidden_entity_name(self):
        entity = {**GOOD_ENTITY, "name": "customer_id"}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[1])
        assert report.failed > 0


# ---------------------------------------------------------------------------
# Check 2: valid_type
# ---------------------------------------------------------------------------

class TestValidType:
    def test_all_valid_types(self):
        dd = _make_dict([GOOD_ENTITY])
        report = validate_dictionary(dd, checks=[2])
        assert report.all_passed

    def test_invalid_type(self):
        entity = {**GOOD_ENTITY, "columns": [
            {"name": "geom", "type": "geometry", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": False},
            {"name": "created_by", "type": "string", "nullable": False},
            {"name": "updated_at", "type": "timestamp", "nullable": False},
            {"name": "updated_by", "type": "string", "nullable": False},
        ]}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[2])
        assert report.failed == 1
        assert "geometry" in report.results[0].message

    def test_parametrised_types_valid(self):
        entity = {**GOOD_ENTITY, "columns": [
            {"name": "price", "type": "decimal(10,2)", "nullable": True},
            {"name": "code", "type": "string(50)", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": False},
            {"name": "created_by", "type": "string", "nullable": False},
            {"name": "updated_at", "type": "timestamp", "nullable": False},
            {"name": "updated_by", "type": "string", "nullable": False},
        ]}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[2])
        assert report.all_passed


# ---------------------------------------------------------------------------
# Check 3: no_duplicate
# ---------------------------------------------------------------------------

class TestNoDuplicate:
    def test_no_duplicates(self):
        dd = _make_dict([GOOD_ENTITY])
        report = validate_dictionary(dd, checks=[3])
        assert report.all_passed

    def test_duplicate_column(self):
        entity = {**GOOD_ENTITY, "columns": [
            {"name": "id", "type": "auto", "nullable": False},
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "created_at", "type": "timestamp", "nullable": False},
            {"name": "created_by", "type": "string", "nullable": False},
            {"name": "updated_at", "type": "timestamp", "nullable": False},
            {"name": "updated_by", "type": "string", "nullable": False},
        ]}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[3])
        assert report.failed == 1
        assert "id" in report.results[0].message


# ---------------------------------------------------------------------------
# Check 4: pii_check
# ---------------------------------------------------------------------------

class TestPiiCheck:
    def test_marked_pii_passes(self):
        entity = {**GOOD_ENTITY, "columns": [
            *GOOD_ENTITY["columns"],
            {"name": "email", "type": "string(320)", "nullable": False, "pii": True},
        ], "security": {"pii_columns": ["email"]}}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[4])
        assert report.all_passed

    def test_unmarked_pii_warns(self):
        entity = {**GOOD_ENTITY, "columns": [
            *GOOD_ENTITY["columns"],
            {"name": "email", "type": "string(320)", "nullable": False},
        ]}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[4])
        # PII is a warning, not an error
        assert report.warnings > 0
        assert report.failed == 0  # warnings don't count as failures

    def test_non_pii_column_passes(self):
        dd = _make_dict([GOOD_ENTITY])
        report = validate_dictionary(dd, checks=[4])
        assert report.all_passed


# ---------------------------------------------------------------------------
# Check 5: naming_convention
# ---------------------------------------------------------------------------

class TestNamingConvention:
    def test_snake_case_passes(self):
        dd = _make_dict([GOOD_ENTITY])
        report = validate_dictionary(dd, checks=[5])
        assert report.all_passed

    def test_camel_case_fails(self):
        entity = {**GOOD_ENTITY, "columns": [
            {"name": "tenantName", "type": "string", "nullable": False},
            {"name": "created_at", "type": "timestamp", "nullable": False},
            {"name": "created_by", "type": "string", "nullable": False},
            {"name": "updated_at", "type": "timestamp", "nullable": False},
            {"name": "updated_by", "type": "string", "nullable": False},
        ]}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[5])
        assert report.failed == 1
        fail = next(r for r in report.results if not r.passed)
        assert "tenant_name" in fail.fix

    def test_uppercase_entity_fails(self):
        entity = {**GOOD_ENTITY, "name": "TENANT"}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[5])
        assert report.failed >= 1


# ---------------------------------------------------------------------------
# Check 6: fk_valid
# ---------------------------------------------------------------------------

class TestFkValid:
    def test_valid_fk(self):
        entity_a = GOOD_ENTITY
        entity_b = {
            "name": "user",
            "schema": "platform",
            "columns": [
                {"name": "id", "type": "auto", "nullable": False},
                {"name": "tenant_id", "type": "string(50)", "nullable": False, "fk": "tenant.tenant_id"},
                {"name": "created_at", "type": "timestamp", "nullable": False},
                {"name": "created_by", "type": "string", "nullable": False},
                {"name": "updated_at", "type": "timestamp", "nullable": False},
                {"name": "updated_by", "type": "string", "nullable": False},
            ],
        }
        dd = _make_dict([entity_a, entity_b])
        report = validate_dictionary(dd, checks=[6])
        assert report.all_passed

    def test_invalid_fk_target(self):
        entity = {
            "name": "orphan",
            "schema": "platform",
            "columns": [
                {"name": "id", "type": "auto", "nullable": False},
                {"name": "ghost_id", "type": "string(50)", "nullable": False, "fk": "nonexistent.id"},
                {"name": "created_at", "type": "timestamp", "nullable": False},
                {"name": "created_by", "type": "string", "nullable": False},
                {"name": "updated_at", "type": "timestamp", "nullable": False},
                {"name": "updated_by", "type": "string", "nullable": False},
            ],
        }
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[6])
        assert report.failed == 1
        assert "nonexistent" in report.results[0].message


# ---------------------------------------------------------------------------
# Check 7: standard_columns
# ---------------------------------------------------------------------------

class TestStandardColumns:
    def test_all_standard_present(self):
        dd = _make_dict([GOOD_ENTITY])
        report = validate_dictionary(dd, checks=[7])
        assert report.all_passed

    def test_missing_updated_by(self):
        entity = {**GOOD_ENTITY, "columns": [
            {"name": "id", "type": "auto", "nullable": False},
            {"name": "created_at", "type": "timestamp", "nullable": False},
            {"name": "created_by", "type": "string", "nullable": False},
            {"name": "updated_at", "type": "timestamp", "nullable": False},
            # missing updated_by
        ]}
        dd = _make_dict([entity])
        report = validate_dictionary(dd, checks=[7])
        assert report.failed == 1
        assert "updated_by" in report.results[0].message
        assert report.results[0].auto_fix is True


# ---------------------------------------------------------------------------
# Suites
# ---------------------------------------------------------------------------

class TestSuites:
    def test_naming_suite(self):
        checks = get_suite_checks("naming")
        assert checks == [1, 5]

    def test_types_suite(self):
        assert get_suite_checks("types") == [2]

    def test_structure_suite(self):
        assert get_suite_checks("structure") == [3, 6, 7]

    def test_security_suite(self):
        assert get_suite_checks("security") == [4]

    def test_invalid_suite(self):
        with pytest.raises(ValueError, match="Unknown suite"):
            get_suite_checks("nonexistent")

    def test_suite_filters_checks(self):
        dd = _make_dict([GOOD_ENTITY])
        report_all = validate_dictionary(dd)
        report_naming = validate_dictionary(dd, checks=get_suite_checks("naming"))
        # Naming suite should run fewer checks
        assert report_naming.total_checks < report_all.total_checks


# ---------------------------------------------------------------------------
# Schema filter
# ---------------------------------------------------------------------------

class TestSchemaFilter:
    def test_filter_by_schema(self):
        entity_a = GOOD_ENTITY  # platform
        entity_b = {
            "name": "agent_registry",
            "schema": "agent_mgmt",
            "columns": [
                {"name": "agent_id", "type": "string(100)", "nullable": False},
                {"name": "created_at", "type": "timestamp", "nullable": False},
                {"name": "created_by", "type": "string", "nullable": False},
                {"name": "updated_at", "type": "timestamp", "nullable": False},
                {"name": "updated_by", "type": "string", "nullable": False},
            ],
        }
        dd = _make_dict([entity_a, entity_b])
        report = validate_dictionary(dd, schema_filter="platform")
        # Should only check tenant (platform), not agent_registry (agent_mgmt)
        entities_checked = {r.entity for r in report.results}
        assert "tenant" in entities_checked
        assert "agent_registry" not in entities_checked


# ---------------------------------------------------------------------------
# Full dictionary integration
# ---------------------------------------------------------------------------

REAL_DICT_PATH = Path("C:/EW/easyway/wiki/guides/db-data-dictionary.json")


@pytest.mark.skipif(not REAL_DICT_PATH.exists(), reason="Real dictionary not found")
class TestRealDictionary:
    def test_validate_real_dictionary(self):
        dd = load_dictionary(REAL_DICT_PATH)
        report = validate_dictionary(dd)
        # The real dictionary should be mostly clean
        assert report.total_checks > 0
        # Print failures for debugging
        for r in report.results:
            if not r.passed:
                print(f"  {r.severity.value}: [{r.check_name}] {r.entity}.{r.column or '*'}: {r.message}")

    def test_naming_suite_on_real(self):
        dd = load_dictionary(REAL_DICT_PATH)
        report = validate_dictionary(dd, checks=get_suite_checks("naming"))
        assert report.failed == 0, "Real dictionary should have valid naming"

    def test_types_suite_on_real(self):
        dd = load_dictionary(REAL_DICT_PATH)
        report = validate_dictionary(dd, checks=get_suite_checks("types"))
        assert report.failed == 0, "Real dictionary should have valid types"

    def test_structure_suite_on_real(self):
        dd = load_dictionary(REAL_DICT_PATH)
        report = validate_dictionary(dd, checks=get_suite_checks("structure"))
        # Print any structure issues
        for r in report.results:
            if not r.passed:
                print(f"  {r.check_name}: {r.entity}: {r.message}")
