"""Tests for hb compile --engine mssql — SQL Server (T-SQL) DDL compiler."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.compile import (
    CompileResult,
    DataDictionary,
    compile_and_write,
    compile_mssql,
    load_dictionary,
    resolve_default,
    resolve_type,
)


# ---------------------------------------------------------------------------
# Fixtures — same dictionary as test_compile but targeting mssql
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
            "description": "Root entity.",
            "multi_tenant": "root",
            "pk": {"columns": ["id"], "type": "auto"},
            "columns": [
                {"name": "id", "type": "auto", "nullable": False, "description": "Surrogate key"},
                {"name": "tenant_id", "type": "string(50)", "nullable": False, "description": "Business key"},
                {"name": "tenant_name", "type": "string(255)", "nullable": False},
                {"name": "status", "type": "string(50)", "nullable": True,
                 "check": "status IN ('ACTIVE', 'SUSPENDED', 'DELETED')"},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
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
            "pk": {"columns": ["agent_id"], "type": "string(100)"},
            "columns": [
                {"name": "agent_id", "type": "string(100)", "nullable": False},
                {"name": "agent_name", "type": "string(255)", "nullable": False},
                {"name": "is_enabled", "type": "boolean", "nullable": False, "default": "true"},
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
                {"name": "agent_id", "type": "string(100)", "nullable": False,
                 "fk": "agent_registry.agent_id"},
                {"name": "status", "type": "string(20)", "nullable": False, "default": "'TODO'",
                 "check": "status IN ('TODO', 'ONGOING', 'DONE', 'FAILED')"},
                {"name": "duration_ms", "type": "integer", "nullable": True},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
            ],
            "indexes": [
                {"name": "ix_exec_agent", "columns": ["agent_id"]},
                {"name": "ix_exec_status", "columns": ["status"]},
            ],
        },
    ],
    "relationships": [
        {"from": "agent_execution", "from_column": "agent_id",
         "to": "agent_registry", "to_column": "agent_id"},
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
# Type & Default resolution for MSSQL
# ---------------------------------------------------------------------------

class TestMssqlTypeResolution:
    def test_auto_to_identity(self):
        tm = MINIMAL_DICT["type_map"]
        assert resolve_type("auto", tm, "mssql") == "BIGINT IDENTITY(1,1)"

    def test_string_to_nvarchar(self):
        tm = MINIMAL_DICT["type_map"]
        assert resolve_type("string(50)", tm, "mssql") == "NVARCHAR(50)"

    def test_boolean_to_bit(self):
        tm = MINIMAL_DICT["type_map"]
        assert resolve_type("boolean", tm, "mssql") == "BIT"

    def test_timestamp_to_datetime2(self):
        tm = MINIMAL_DICT["type_map"]
        assert resolve_type("timestamp", tm, "mssql") == "DATETIME2"

    def test_json_to_nvarchar_max(self):
        tm = MINIMAL_DICT["type_map"]
        assert resolve_type("json", tm, "mssql") == "NVARCHAR(MAX)"

    def test_decimal_parametrised(self):
        tm = MINIMAL_DICT["type_map"]
        assert resolve_type("decimal(10,2)", tm, "mssql") == "DECIMAL(10,2)"


class TestMssqlDefaultResolution:
    def test_now_to_sysutcdatetime(self):
        dm = MINIMAL_DICT["default_map"]
        assert resolve_default("now()", dm, "mssql") == "SYSUTCDATETIME()"

    def test_true_to_1(self):
        dm = MINIMAL_DICT["default_map"]
        assert resolve_default("true", dm, "mssql") == "1"

    def test_uuid_to_newid(self):
        dm = MINIMAL_DICT["default_map"]
        assert resolve_default("uuid()", dm, "mssql") == "NEWID()"


# ---------------------------------------------------------------------------
# compile_mssql
# ---------------------------------------------------------------------------

class TestCompileMssql:
    def test_result_counts(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        assert result.engine == "mssql"
        assert result.entity_count == 3
        assert result.index_count > 0
        assert result.fk_count == 1

    def test_file_count(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        names = [f["name"] for f in result.files]
        assert "001_schemas.sql" in names
        assert "002_tables.sql" in names
        assert "003_indexes.sql" in names
        assert "004_foreign_keys.sql" in names
        assert "005_comments.sql" in names

    def test_schema_idempotent(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        schemas_file = next(f for f in result.files if f["name"] == "001_schemas.sql")
        assert "IF NOT EXISTS" in schemas_file["content"]
        assert "EXEC('CREATE SCHEMA" in schemas_file["content"]
        assert "GO" in schemas_file["content"]

    def test_table_idempotent(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "IF OBJECT_ID" in tables_file["content"]
        assert "IS NULL" in tables_file["content"]

    def test_identity_in_ddl(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "IDENTITY(1,1)" in tables_file["content"]

    def test_nvarchar_in_ddl(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "NVARCHAR(50)" in tables_file["content"]

    def test_datetime2_in_ddl(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "DATETIME2" in tables_file["content"]

    def test_default_sysutcdatetime(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "SYSUTCDATETIME()" in tables_file["content"]

    def test_bit_default_1(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "DEFAULT 1" in tables_file["content"]

    def test_check_constraint(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "CHECK" in tables_file["content"]
        assert "'ACTIVE'" in tables_file["content"]

    def test_pk_constraint(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "PRIMARY KEY" in tables_file["content"]
        assert "PK_tenant" in tables_file["content"]

    def test_fk_idempotent(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        fk_file = next(f for f in result.files if f["name"] == "004_foreign_keys.sql")
        assert "IF OBJECT_ID" in fk_file["content"]
        assert "FK_agent_execution_agent_id" in fk_file["content"]
        assert "REFERENCES" in fk_file["content"]

    def test_index_idempotent(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        idx_file = next(f for f in result.files if f["name"] == "003_indexes.sql")
        assert "IF NOT EXISTS" in idx_file["content"]
        assert "sys.indexes" in idx_file["content"]
        assert "GO" in idx_file["content"]

    def test_unique_index(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        idx_file = next(f for f in result.files if f["name"] == "003_indexes.sql")
        assert "UNIQUE INDEX" in idx_file["content"]
        assert "ux_tenant_tenant_id" in idx_file["content"]

    def test_comments_via_extended_property(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        comments_file = next(f for f in result.files if f["name"] == "005_comments.sql")
        assert "sp_addextendedproperty" in comments_file["content"]
        assert "MS_Description" in comments_file["content"]

    def test_schema_filter(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary, schema_filter="platform")
        assert result.entity_count == 1

    def test_square_bracket_quoting(self, dictionary: DataDictionary):
        result = compile_mssql(dictionary)
        tables_file = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "[platform].[tenant]" in tables_file["content"]
        assert "[id]" in tables_file["content"]


# ---------------------------------------------------------------------------
# compile_and_write with mssql engine
# ---------------------------------------------------------------------------

class TestCompileAndWriteMssql:
    def test_writes_files(self, dictionary: DataDictionary, tmp_path: Path):
        result = compile_and_write(dictionary, engine="mssql", profile="essential", output_dir=tmp_path)
        assert result.engine == "mssql"
        files = list(tmp_path.glob("*.sql"))
        assert len(files) >= 4  # schemas, tables, indexes, fk, comments

    def test_no_security_ddl_for_mssql(self, dictionary: DataDictionary, tmp_path: Path):
        # Security DDL is PG-only for now
        result = compile_and_write(dictionary, engine="mssql", profile="standard", output_dir=tmp_path)
        assert result.security_file_count == 0


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

class TestMssqlCompileCLI:
    def test_compile_mssql_engine(self, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        out_dir = tmp_path / "sql"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "compile",
            "-e", "mssql",
            "-d", str(dict_file),
            "-o", str(out_dir),
        ])

        assert result.exit_code == 0
        assert "MSSQL" in result.output
        assert (out_dir / "002_tables.sql").exists()

    def test_compile_mssql_json_output(self, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        out_dir = tmp_path / "sql"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "compile",
            "-e", "mssql",
            "-d", str(dict_file),
            "-o", str(out_dir),
            "-j",
        ])

        assert result.exit_code == 0
        # Find JSON in output
        lines = result.output.strip().split("\n")
        json_start = next(i for i, l in enumerate(lines) if l.strip().startswith("{"))
        output = json.loads("\n".join(lines[json_start:]))
        assert output["engine"] == "mssql"
        assert output["entities"] == 3


# ---------------------------------------------------------------------------
# Real dictionary integration
# ---------------------------------------------------------------------------

REAL_DICT_PATH = Path("C:/EW/easyway/wiki/guides/db-data-dictionary.json")


@pytest.mark.skipif(not REAL_DICT_PATH.exists(), reason="Real dictionary not found")
class TestRealDictionary:
    def test_compile_real_mssql(self):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_mssql(dd)
        assert result.engine == "mssql"
        assert result.entity_count > 0
        assert len(result.files) >= 4

    def test_compile_real_to_disk(self, tmp_path: Path):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_and_write(dd, engine="mssql", profile="essential", output_dir=tmp_path)
        assert result.entity_count > 0
        tables_file = (tmp_path / "002_tables.sql").read_text(encoding="utf-8")
        assert "IF OBJECT_ID" in tables_file
        assert "IDENTITY(1,1)" in tables_file
