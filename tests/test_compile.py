"""Tests for hb compile — Dictionary Reader + PG Compiler."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from app.core.compile import (
    CompileResult,
    DataDictionary,
    compile_and_write,
    compile_pg,
    load_dictionary,
    resolve_default,
    resolve_type,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal dictionary
# ---------------------------------------------------------------------------

MINIMAL_DICT = {
    "type_map": {
        "uuid": {"pg": "TEXT", "mssql": "UNIQUEIDENTIFIER"},
        "string": {"pg": "TEXT", "mssql": "NVARCHAR(255)"},
        "string(n)": {"pg": "VARCHAR({n})", "mssql": "NVARCHAR({n})"},
        "integer": {"pg": "INTEGER", "mssql": "INT"},
        "auto": {"pg": "BIGSERIAL", "mssql": "BIGINT IDENTITY(1,1)"},
        "boolean": {"pg": "BOOLEAN", "mssql": "BIT"},
        "timestamp": {"pg": "TIMESTAMPTZ", "mssql": "DATETIME2"},
        "date": {"pg": "DATE", "mssql": "DATE"},
        "decimal(p,s)": {"pg": "NUMERIC({p},{s})", "mssql": "DECIMAL({p},{s})"},
        "json": {"pg": "JSONB", "mssql": "NVARCHAR(MAX)"},
        "text_large": {"pg": "TEXT", "mssql": "NVARCHAR(MAX)"},
        "long": {"pg": "BIGINT", "mssql": "BIGINT"},
    },
    "default_map": {
        "now()": {"pg": "NOW()", "mssql": "SYSUTCDATETIME()"},
        "uuid()": {"pg": "gen_random_uuid()::TEXT", "mssql": "NEWID()"},
        "true": {"pg": "TRUE", "mssql": "1"},
        "false": {"pg": "FALSE", "mssql": "0"},
        "empty_json": {"pg": "'{}'::JSONB", "mssql": "'{}'"},
    },
    "security_profiles": {},
    "standard_columns": [],
    "schemas": [
        {"name": "platform", "description": "Portal schema"},
        {"name": "agent_mgmt", "description": "Agent management"},
    ],
    "entities": [
        {
            "id": "tenant",
            "name": "tenant",
            "schema": "platform",
            "type": "DIM",
            "description": "Root entity — every row belongs to a tenant.",
            "multi_tenant": "root",
            "pk": {"columns": ["id"], "type": "auto"},
            "columns": [
                {"name": "id", "type": "auto", "nullable": False, "description": "Surrogate key"},
                {"name": "tenant_id", "type": "string(50)", "nullable": False, "description": "Business key"},
                {"name": "tenant_name", "type": "string(255)", "nullable": False, "description": "Organization name"},
                {"name": "status", "type": "string(50)", "nullable": True, "check": "status IN ('ACTIVE', 'SUSPENDED', 'DELETED')"},
                {"name": "ext_attributes", "type": "json", "nullable": True},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
                {"name": "updated_at", "type": "timestamp", "nullable": False, "default": "now()"},
            ],
            "indexes": [
                {"name": "ux_tenant_tenant_id", "columns": ["tenant_id"], "unique": True},
            ],
        },
        {
            "id": "agent_registry",
            "name": "agent_registry",
            "schema": "agent_mgmt",
            "type": "DIM",
            "description": "Agent master registry.",
            "multi_tenant": False,
            "pk": {"columns": ["agent_id"], "type": "string(100)"},
            "columns": [
                {"name": "agent_id", "type": "string(100)", "nullable": False, "description": "Agent ID"},
                {"name": "agent_name", "type": "string(255)", "nullable": False},
                {"name": "is_enabled", "type": "boolean", "nullable": False, "default": "true"},
                {"name": "llm_temperature", "type": "decimal(3,2)", "nullable": True},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
            ],
            "indexes": [
                {"name": "ix_agent_enabled", "columns": ["is_enabled"]},
            ],
        },
        {
            "id": "agent_execution",
            "name": "agent_execution",
            "schema": "agent_mgmt",
            "type": "FACT",
            "description": "Agent execution log.",
            "pk": {"columns": ["execution_id"], "type": "auto"},
            "columns": [
                {"name": "execution_id", "type": "auto", "nullable": False},
                {"name": "agent_id", "type": "string(100)", "nullable": False, "fk": "agent_registry.agent_id"},
                {"name": "status", "type": "string(20)", "nullable": False, "default": "'TODO'",
                 "check": "status IN ('TODO', 'ONGOING', 'DONE', 'FAILED', 'CANCELLED')"},
                {"name": "duration_ms", "type": "integer", "nullable": True},
                {"name": "success", "type": "boolean", "nullable": True},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
            ],
            "indexes": [
                {"name": "ix_exec_agent", "columns": ["agent_id"]},
                {"name": "ix_exec_status", "columns": ["status"]},
            ],
        },
    ],
    "relationships": [
        {"from": "agent_execution", "from_column": "agent_id", "to": "agent_registry", "to_column": "agent_id"},
    ],
}


@pytest.fixture
def dictionary() -> DataDictionary:
    return DataDictionary.model_validate(MINIMAL_DICT)


@pytest.fixture
def dict_file(tmp_path: Path) -> Path:
    p = tmp_path / "dict.json"
    p.write_text(json.dumps(MINIMAL_DICT), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Type resolution
# ---------------------------------------------------------------------------

class TestResolveType:
    def test_direct_type(self, dictionary: DataDictionary):
        assert resolve_type("uuid", dictionary.type_map, "pg") == "TEXT"
        assert resolve_type("integer", dictionary.type_map, "pg") == "INTEGER"
        assert resolve_type("json", dictionary.type_map, "pg") == "JSONB"
        assert resolve_type("timestamp", dictionary.type_map, "pg") == "TIMESTAMPTZ"
        assert resolve_type("auto", dictionary.type_map, "pg") == "BIGSERIAL"
        assert resolve_type("boolean", dictionary.type_map, "pg") == "BOOLEAN"

    def test_parametrised_string(self, dictionary: DataDictionary):
        assert resolve_type("string(50)", dictionary.type_map, "pg") == "VARCHAR(50)"
        assert resolve_type("string(255)", dictionary.type_map, "pg") == "VARCHAR(255)"
        assert resolve_type("string(100)", dictionary.type_map, "mssql") == "NVARCHAR(100)"

    def test_parametrised_decimal(self, dictionary: DataDictionary):
        assert resolve_type("decimal(10,6)", dictionary.type_map, "pg") == "NUMERIC(10,6)"
        assert resolve_type("decimal(3,2)", dictionary.type_map, "pg") == "NUMERIC(3,2)"
        assert resolve_type("decimal(18,6)", dictionary.type_map, "mssql") == "DECIMAL(18,6)"

    def test_unknown_type_fallback(self, dictionary: DataDictionary):
        assert resolve_type("geometry", dictionary.type_map, "pg") == "GEOMETRY"


# ---------------------------------------------------------------------------
# Default resolution
# ---------------------------------------------------------------------------

class TestResolveDefault:
    def test_known_defaults(self, dictionary: DataDictionary):
        assert resolve_default("now()", dictionary.default_map, "pg") == "NOW()"
        assert resolve_default("uuid()", dictionary.default_map, "pg") == "gen_random_uuid()::TEXT"
        assert resolve_default("true", dictionary.default_map, "pg") == "TRUE"
        assert resolve_default("false", dictionary.default_map, "pg") == "FALSE"

    def test_literal_passthrough(self, dictionary: DataDictionary):
        assert resolve_default("'system'", dictionary.default_map, "pg") == "'system'"
        assert resolve_default("'MANUAL'", dictionary.default_map, "pg") == "'MANUAL'"
        assert resolve_default("0", dictionary.default_map, "pg") == "0"


# ---------------------------------------------------------------------------
# Dictionary loading
# ---------------------------------------------------------------------------

class TestLoadDictionary:
    def test_load_from_file(self, dict_file: Path):
        dd = load_dictionary(dict_file)
        assert len(dd.entities) == 3
        assert dd.entities[0].name == "tenant"
        assert dd.entities[0].schema_name == "platform"

    def test_entity_columns(self, dictionary: DataDictionary):
        tenant = dictionary.entities[0]
        assert len(tenant.columns) == 7
        assert tenant.columns[0].name == "id"
        assert tenant.columns[0].type == "auto"

    def test_entity_indexes(self, dictionary: DataDictionary):
        tenant = dictionary.entities[0]
        assert len(tenant.indexes) == 1
        assert tenant.indexes[0].unique is True

    def test_entity_fk(self, dictionary: DataDictionary):
        execution = dictionary.entities[2]
        fk_col = execution.columns[1]
        assert fk_col.fk == "agent_registry.agent_id"


# ---------------------------------------------------------------------------
# PG Compiler — DDL generation
# ---------------------------------------------------------------------------

class TestCompilePg:
    def test_result_counts(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        assert result.engine == "pg"
        assert result.entity_count == 3
        assert result.index_count == 4  # 1 + 1 + 2
        assert result.fk_count == 1
        assert result.check_count == 2  # tenant.status + agent_execution.status

    def test_file_count(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        names = [f["name"] for f in result.files]
        assert "001_schemas.sql" in names
        assert "002_tables.sql" in names
        assert "003_indexes.sql" in names
        assert "004_foreign_keys.sql" in names
        assert "005_comments.sql" in names

    def test_schema_filter(self, dictionary: DataDictionary):
        result = compile_pg(dictionary, schema_filter="platform")
        assert result.entity_count == 1

    def test_schemas_sql_idempotent(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        schemas_file = next(f for f in result.files if f["name"] == "001_schemas.sql")
        assert "CREATE SCHEMA IF NOT EXISTS platform;" in schemas_file["content"]
        assert "CREATE SCHEMA IF NOT EXISTS agent_mgmt;" in schemas_file["content"]

    def test_table_if_not_exists(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "CREATE TABLE IF NOT EXISTS platform.tenant" in tables_file["content"]
        assert "CREATE TABLE IF NOT EXISTS agent_mgmt.agent_registry" in tables_file["content"]

    def test_type_translation_in_ddl(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        tables = next(f for f in result.files if f["name"] == "002_tables.sql")["content"]
        # tenant.id should be BIGSERIAL
        assert "BIGSERIAL" in tables
        # tenant.tenant_id should be VARCHAR(50)
        assert "VARCHAR(50)" in tables
        # agent_registry.llm_temperature should be NUMERIC(3,2)
        assert "NUMERIC(3,2)" in tables
        # ext_attributes should be JSONB
        assert "JSONB" in tables
        # created_at should be TIMESTAMPTZ
        assert "TIMESTAMPTZ" in tables

    def test_default_translation_in_ddl(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        tables = next(f for f in result.files if f["name"] == "002_tables.sql")["content"]
        assert "DEFAULT NOW()" in tables
        assert "DEFAULT TRUE" in tables
        assert "DEFAULT 'TODO'" in tables

    def test_check_constraint_in_ddl(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        tables = next(f for f in result.files if f["name"] == "002_tables.sql")["content"]
        assert "CHECK (status IN ('ACTIVE', 'SUSPENDED', 'DELETED'))" in tables
        assert "CHECK (status IN ('TODO', 'ONGOING', 'DONE', 'FAILED', 'CANCELLED'))" in tables

    def test_pk_in_ddl(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        tables = next(f for f in result.files if f["name"] == "002_tables.sql")["content"]
        # Single-column PKs should have PRIMARY KEY inline
        assert "id BIGSERIAL PRIMARY KEY" in tables
        assert "agent_id VARCHAR(100) PRIMARY KEY" in tables

    def test_fk_as_alter_table(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        fk_file = next(f for f in result.files if f["name"] == "004_foreign_keys.sql")
        content = fk_file["content"]
        assert "ALTER TABLE agent_mgmt.agent_execution" in content
        assert "ADD CONSTRAINT fk_agent_execution_agent_id" in content
        assert "REFERENCES agent_mgmt.agent_registry(agent_id)" in content

    def test_index_if_not_exists(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        idx_file = next(f for f in result.files if f["name"] == "003_indexes.sql")
        content = idx_file["content"]
        assert "CREATE UNIQUE INDEX IF NOT EXISTS ux_tenant_tenant_id" in content
        assert "CREATE INDEX IF NOT EXISTS ix_agent_enabled" in content

    def test_comments(self, dictionary: DataDictionary):
        result = compile_pg(dictionary)
        comments_file = next(f for f in result.files if f["name"] == "005_comments.sql")
        content = comments_file["content"]
        assert "COMMENT ON TABLE platform.tenant IS" in content
        assert "COMMENT ON COLUMN platform.tenant.tenant_id IS" in content


# ---------------------------------------------------------------------------
# Compile and write to disk
# ---------------------------------------------------------------------------

class TestCompileAndWrite:
    def test_writes_files(self, dict_file: Path, tmp_path: Path):
        dd = load_dictionary(dict_file)
        out = tmp_path / "migrations"
        result = compile_and_write(dd, engine="pg", profile="essential", output_dir=out)

        assert out.exists()
        assert (out / "001_schemas.sql").exists()
        assert (out / "002_tables.sql").exists()
        assert result.entity_count == 3

    def test_unsupported_engine(self, dictionary: DataDictionary, tmp_path: Path):
        with pytest.raises(ValueError, match="not yet supported"):
            compile_and_write(dictionary, engine="snowflake", profile="essential", output_dir=tmp_path / "out")


# ---------------------------------------------------------------------------
# Full dictionary integration — load the real dictionary
# ---------------------------------------------------------------------------

REAL_DICT_PATH = Path("C:/EW/easyway/wiki/guides/db-data-dictionary.json")


@pytest.mark.skipif(not REAL_DICT_PATH.exists(), reason="Real dictionary not found")
class TestRealDictionary:
    def test_load_real_dictionary(self):
        dd = load_dictionary(REAL_DICT_PATH)
        assert len(dd.entities) >= 18  # 20 entities in the dictionary
        assert len(dd.type_map) >= 10

    def test_compile_real_dictionary(self):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_pg(dd, profile="essential")
        assert result.entity_count >= 18
        assert result.index_count > 0
        assert result.fk_count > 0
        assert result.check_count > 0
        assert result.comment_count > 0

    def test_compile_real_to_disk(self, tmp_path: Path):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_and_write(dd, engine="pg", profile="essential", output_dir=tmp_path / "pg")
        for f in result.files:
            path = tmp_path / "pg" / f["name"]
            assert path.exists()
            content = path.read_text(encoding="utf-8")
            assert len(content) > 0
