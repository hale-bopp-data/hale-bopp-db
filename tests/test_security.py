"""Tests for hb compile security — Security DDL generator (3 profiles)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.compile import DataDictionary, load_dictionary, compile_and_write
from app.core.security import SecurityDDL, generate_security_pg


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_dict(entities: list[dict], **overrides) -> DataDictionary:
    base = {
        "type_map": {
            "uuid": {"pg": "TEXT"},
            "string": {"pg": "TEXT"},
            "string(n)": {"pg": "VARCHAR({n})"},
            "integer": {"pg": "INTEGER"},
            "auto": {"pg": "BIGSERIAL"},
            "boolean": {"pg": "BOOLEAN"},
            "timestamp": {"pg": "TIMESTAMPTZ"},
            "json": {"pg": "JSONB"},
            "text_large": {"pg": "TEXT"},
            "long": {"pg": "BIGINT"},
            "date": {"pg": "DATE"},
            "decimal(p,s)": {"pg": "NUMERIC({p},{s})"},
        },
        "default_map": {
            "now()": {"pg": "NOW()"},
            "true": {"pg": "TRUE"},
            "false": {"pg": "FALSE"},
            "uuid()": {"pg": "gen_random_uuid()::TEXT"},
        },
        "security_profiles": {
            "essential": {"capabilities": {
                "rls": False, "pii_tagging": False, "dynamic_masking": False, "retention_policy": False,
            }},
            "standard": {"capabilities": {
                "rls": True, "pii_tagging": True, "dynamic_masking": False, "retention_policy": False,
            }},
            "enterprise": {"capabilities": {
                "rls": True, "pii_tagging": True, "dynamic_masking": True, "retention_policy": True,
            }},
        },
        "rls_map": {
            "pg": {"mechanism": "CREATE POLICY", "session_var": "current_setting('app.tenant_id', TRUE)"},
        },
        "masking_map": {
            "pg": {"mechanism": "View-based o pgcrypto"},
        },
        "entities": entities,
    }
    base.update(overrides)
    return DataDictionary.model_validate(base)


TENANT_ENTITY = {
    "name": "tenant",
    "schema": "platform",
    "type": "DIM",
    "multi_tenant": "root",
    "security": {"rls": False, "rls_note": "Root entity"},
    "columns": [
        {"name": "id", "type": "auto", "nullable": False},
        {"name": "tenant_id", "type": "string(50)", "nullable": False},
        {"name": "created_at", "type": "timestamp", "nullable": False},
        {"name": "created_by", "type": "string", "nullable": False},
        {"name": "updated_at", "type": "timestamp", "nullable": False},
        {"name": "updated_by", "type": "string", "nullable": False},
    ],
}

USER_ENTITY = {
    "name": "user",
    "schema": "platform",
    "type": "DIM",
    "multi_tenant": True,
    "security": {
        "rls": True,
        "rls_policy": "RLS_TENANT_POLICY_USERS",
        "pii_columns": ["email", "display_name"],
        "masking": {"email": "partial", "display_name": "full"},
    },
    "columns": [
        {"name": "id", "type": "auto", "nullable": False},
        {"name": "tenant_id", "type": "string(50)", "nullable": False},
        {"name": "email", "type": "string(320)", "nullable": False, "pii": True},
        {"name": "display_name", "type": "string(100)", "nullable": True, "pii": True},
        {"name": "status", "type": "string(50)", "nullable": True},
        {"name": "created_at", "type": "timestamp", "nullable": False},
        {"name": "created_by", "type": "string", "nullable": False},
        {"name": "updated_at", "type": "timestamp", "nullable": False},
        {"name": "updated_by", "type": "string", "nullable": False},
    ],
}

LOG_ENTITY = {
    "name": "log_audit",
    "schema": "platform",
    "type": "LOG",
    "multi_tenant": True,
    "security": {"rls": False, "pii_columns": ["actor"], "masking": {"actor": "partial"}},
    "columns": [
        {"name": "id", "type": "auto", "nullable": False},
        {"name": "tenant_id", "type": "string(50)", "nullable": True},
        {"name": "actor", "type": "string(255)", "nullable": True, "pii": True},
        {"name": "event_time", "type": "timestamp", "nullable": False, "default": "now()"},
        {"name": "created_at", "type": "timestamp", "nullable": False},
        {"name": "created_by", "type": "string", "nullable": False},
        {"name": "updated_at", "type": "timestamp", "nullable": False},
        {"name": "updated_by", "type": "string", "nullable": False},
    ],
}


# ---------------------------------------------------------------------------
# Profile: Essential — no security DDL
# ---------------------------------------------------------------------------

class TestEssentialProfile:
    def test_no_security_ddl(self):
        dd = _make_dict([TENANT_ENTITY, USER_ENTITY])
        result = generate_security_pg(dd, profile="essential")
        assert not result.has_content
        assert result.to_files() == []


# ---------------------------------------------------------------------------
# Profile: Standard — RLS + PII tagging
# ---------------------------------------------------------------------------

class TestStandardProfile:
    def test_rls_generated_for_rls_true(self):
        dd = _make_dict([TENANT_ENTITY, USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        assert len(result.rls_statements) > 0
        assert len(result.rls_setup) > 0

    def test_rls_enables_on_table(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        setup_text = "\n".join(result.rls_setup)
        assert "ALTER TABLE platform.user ENABLE ROW LEVEL SECURITY" in setup_text
        assert "ALTER TABLE platform.user FORCE ROW LEVEL SECURITY" in setup_text

    def test_rls_creates_policy(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        rls_text = "\n".join(result.rls_statements)
        assert "CREATE POLICY RLS_TENANT_POLICY_USERS ON platform.user" in rls_text
        assert "current_setting('app.tenant_id', TRUE)" in rls_text

    def test_rls_idempotent(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        rls_text = "\n".join(result.rls_statements)
        assert "IF NOT EXISTS" in rls_text

    def test_no_rls_on_root_entity(self):
        dd = _make_dict([TENANT_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        assert len(result.rls_statements) == 0

    def test_pii_tagging_generated(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        assert len(result.pii_comments) > 0
        pii_text = "\n".join(result.pii_comments)
        assert "COMMENT ON COLUMN platform.user.email" in pii_text
        assert "PII: true" in pii_text

    def test_pii_includes_masking_type(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        pii_text = "\n".join(result.pii_comments)
        assert "masking: partial" in pii_text  # email
        assert "masking: full" in pii_text  # display_name

    def test_no_masking_in_standard(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        assert len(result.masking_statements) == 0

    def test_files_generated(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="standard")
        files = result.to_files()
        names = [f["name"] for f in files]
        assert "006_rls.sql" in names
        assert "007_pii_tagging.sql" in names
        assert "008_masking.sql" not in names


# ---------------------------------------------------------------------------
# Profile: Enterprise — RLS + PII + masking + retention
# ---------------------------------------------------------------------------

class TestEnterpriseProfile:
    def test_masking_views_generated(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        assert len(result.masking_statements) > 0

    def test_masking_view_name(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        mask_text = "\n".join(result.masking_statements)
        assert "CREATE OR REPLACE VIEW platform.v_user_masked" in mask_text

    def test_masking_full_redaction(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        mask_text = "\n".join(result.masking_statements)
        # display_name has full masking
        assert "'***'" in mask_text

    def test_masking_partial_email(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        mask_text = "\n".join(result.masking_statements)
        # email has partial masking — shows domain
        assert "SPLIT_PART" in mask_text

    def test_masking_admin_bypass(self):
        dd = _make_dict([USER_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        mask_text = "\n".join(result.masking_statements)
        assert "current_setting('app.role', TRUE) = 'admin'" in mask_text

    def test_retention_function(self):
        dd = _make_dict([LOG_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        assert len(result.retention_statements) > 0
        ret_text = "\n".join(result.retention_statements)
        assert "hb_apply_retention" in ret_text

    def test_retention_for_log_entity(self):
        dd = _make_dict([LOG_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        ret_text = "\n".join(result.retention_statements)
        assert "log_audit" in ret_text

    def test_enterprise_files(self):
        dd = _make_dict([USER_ENTITY, LOG_ENTITY])
        result = generate_security_pg(dd, profile="enterprise")
        files = result.to_files()
        names = [f["name"] for f in files]
        assert "006_rls.sql" in names
        assert "007_pii_tagging.sql" in names
        assert "008_masking.sql" in names
        assert "009_retention.sql" in names


# ---------------------------------------------------------------------------
# Multi-tenant auto-detection
# ---------------------------------------------------------------------------

class TestMultiTenantAutoDetect:
    def test_multi_tenant_true_gets_rls(self):
        """Entity with multi_tenant=true and tenant_id should get RLS even without security.rls=true."""
        entity = {
            "name": "config",
            "schema": "platform",
            "type": "CONFIG",
            "multi_tenant": True,
            "security": {},  # No explicit rls=true
            "columns": [
                {"name": "id", "type": "auto", "nullable": False},
                {"name": "tenant_id", "type": "string(50)", "nullable": False},
                {"name": "created_at", "type": "timestamp", "nullable": False},
                {"name": "created_by", "type": "string", "nullable": False},
                {"name": "updated_at", "type": "timestamp", "nullable": False},
                {"name": "updated_by", "type": "string", "nullable": False},
            ],
        }
        dd = _make_dict([entity])
        result = generate_security_pg(dd, profile="standard")
        assert len(result.rls_statements) > 0


# ---------------------------------------------------------------------------
# Integration with compile_and_write
# ---------------------------------------------------------------------------

class TestCompileWithSecurity:
    def test_essential_no_security_files(self, tmp_path: Path):
        dd = _make_dict([TENANT_ENTITY, USER_ENTITY])
        result = compile_and_write(dd, engine="pg", profile="essential", output_dir=tmp_path / "out")
        names = [f["name"] for f in result.files]
        assert "006_rls.sql" not in names

    def test_standard_adds_security_files(self, tmp_path: Path):
        dd = _make_dict([TENANT_ENTITY, USER_ENTITY])
        result = compile_and_write(dd, engine="pg", profile="standard", output_dir=tmp_path / "out")
        names = [f["name"] for f in result.files]
        assert "006_rls.sql" in names
        assert "007_pii_tagging.sql" in names
        # Verify files exist on disk
        assert (tmp_path / "out" / "006_rls.sql").exists()
        assert (tmp_path / "out" / "007_pii_tagging.sql").exists()

    def test_enterprise_adds_all_security(self, tmp_path: Path):
        dd = _make_dict([USER_ENTITY, LOG_ENTITY])
        result = compile_and_write(dd, engine="pg", profile="enterprise", output_dir=tmp_path / "out")
        names = [f["name"] for f in result.files]
        assert "006_rls.sql" in names
        assert "008_masking.sql" in names
        assert "009_retention.sql" in names


# ---------------------------------------------------------------------------
# Real dictionary
# ---------------------------------------------------------------------------

REAL_DICT_PATH = Path("C:/EW/easyway/wiki/guides/db-data-dictionary.json")


@pytest.mark.skipif(not REAL_DICT_PATH.exists(), reason="Real dictionary not found")
class TestRealDictionary:
    def test_essential_no_security(self):
        dd = load_dictionary(REAL_DICT_PATH)
        result = generate_security_pg(dd, profile="essential")
        assert not result.has_content

    def test_standard_has_rls_and_pii(self):
        dd = load_dictionary(REAL_DICT_PATH)
        result = generate_security_pg(dd, profile="standard")
        assert len(result.rls_statements) > 0
        assert len(result.pii_comments) > 0
        assert len(result.masking_statements) == 0

    def test_enterprise_has_masking(self):
        dd = load_dictionary(REAL_DICT_PATH)
        result = generate_security_pg(dd, profile="enterprise")
        assert len(result.rls_statements) > 0
        assert len(result.pii_comments) > 0
        assert len(result.masking_statements) > 0
        assert len(result.retention_statements) > 0

    def test_compile_standard_to_disk(self, tmp_path: Path):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_and_write(dd, engine="pg", profile="standard", output_dir=tmp_path / "std")
        rls_file = tmp_path / "std" / "006_rls.sql"
        assert rls_file.exists()
        content = rls_file.read_text(encoding="utf-8")
        assert "CREATE POLICY" in content
        assert "ENABLE ROW LEVEL SECURITY" in content

    def test_compile_enterprise_to_disk(self, tmp_path: Path):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_and_write(dd, engine="pg", profile="enterprise", output_dir=tmp_path / "ent")
        assert (tmp_path / "ent" / "006_rls.sql").exists()
        assert (tmp_path / "ent" / "007_pii_tagging.sql").exists()
        assert (tmp_path / "ent" / "008_masking.sql").exists()
        assert (tmp_path / "ent" / "009_retention.sql").exists()
