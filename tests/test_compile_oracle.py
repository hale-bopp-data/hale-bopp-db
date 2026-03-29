"""Tests for hb compile --engine oracle — Oracle PL/SQL DDL compiler."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.compile import (
    CompileResult,
    DataDictionary,
    compile_and_write,
    compile_oracle,
    resolve_default,
    resolve_type,
)


# ---------------------------------------------------------------------------
# Fixtures — dictionary with Oracle type mappings
# ---------------------------------------------------------------------------

ORACLE_DICT = {
    "type_map": {
        "uuid": {"pg": "TEXT", "mssql": "UNIQUEIDENTIFIER", "oracle": "VARCHAR2(36)"},
        "string": {"pg": "TEXT", "mssql": "NVARCHAR(255)", "oracle": "VARCHAR2(255)"},
        "string(n)": {"pg": "VARCHAR({n})", "mssql": "NVARCHAR({n})", "oracle": "VARCHAR2({n})"},
        "integer": {"pg": "INTEGER", "mssql": "INT", "oracle": "NUMBER(10)"},
        "auto": {"pg": "BIGSERIAL", "mssql": "BIGINT IDENTITY(1,1)", "oracle": "NUMBER(19) GENERATED ALWAYS AS IDENTITY"},
        "boolean": {"pg": "BOOLEAN", "mssql": "BIT", "oracle": "NUMBER(1)"},
        "timestamp": {"pg": "TIMESTAMPTZ", "mssql": "DATETIME2", "oracle": "TIMESTAMP WITH TIME ZONE"},
        "date": {"pg": "DATE", "mssql": "DATE", "oracle": "DATE"},
        "decimal(p,s)": {"pg": "NUMERIC({p},{s})", "mssql": "DECIMAL({p},{s})", "oracle": "NUMBER({p},{s})"},
        "json": {"pg": "JSONB", "mssql": "NVARCHAR(MAX)", "oracle": "CLOB"},
        "text_large": {"pg": "TEXT", "mssql": "NVARCHAR(MAX)", "oracle": "CLOB"},
        "long": {"pg": "BIGINT", "mssql": "BIGINT", "oracle": "NUMBER(19)"},
    },
    "default_map": {
        "now()": {"pg": "NOW()", "mssql": "SYSUTCDATETIME()", "oracle": "SYSTIMESTAMP"},
        "uuid()": {"pg": "gen_random_uuid()::TEXT", "mssql": "NEWID()", "oracle": "SYS_GUID()"},
        "true": {"pg": "TRUE", "mssql": "1", "oracle": "1"},
        "false": {"pg": "FALSE", "mssql": "0", "oracle": "0"},
        "empty_json": {"pg": "'{}'::JSONB", "mssql": "'{}'", "oracle": "'{}'"},
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
                {"name": "duration_ms", "type": "integer", "nullable": True},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
            ],
            "indexes": [
                {"name": "ix_exec_agent", "columns": ["agent_id"]},
            ],
        },
    ],
    "relationships": [],
}


@pytest.fixture
def dictionary() -> DataDictionary:
    return DataDictionary.model_validate(ORACLE_DICT)


@pytest.fixture
def dict_file(tmp_path: Path) -> Path:
    p = tmp_path / "dict.json"
    p.write_text(json.dumps(ORACLE_DICT), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Type & Default resolution
# ---------------------------------------------------------------------------

class TestOracleTypeResolution:
    def test_auto_to_identity(self):
        tm = ORACLE_DICT["type_map"]
        result = resolve_type("auto", tm, "oracle")
        assert "GENERATED ALWAYS AS IDENTITY" in result

    def test_string_to_varchar2(self):
        tm = ORACLE_DICT["type_map"]
        assert resolve_type("string(50)", tm, "oracle") == "VARCHAR2(50)"

    def test_boolean_to_number1(self):
        tm = ORACLE_DICT["type_map"]
        assert resolve_type("boolean", tm, "oracle") == "NUMBER(1)"

    def test_timestamp_to_tz(self):
        tm = ORACLE_DICT["type_map"]
        assert resolve_type("timestamp", tm, "oracle") == "TIMESTAMP WITH TIME ZONE"

    def test_json_to_clob(self):
        tm = ORACLE_DICT["type_map"]
        assert resolve_type("json", tm, "oracle") == "CLOB"

    def test_decimal_parametrised(self):
        tm = ORACLE_DICT["type_map"]
        assert resolve_type("decimal(10,2)", tm, "oracle") == "NUMBER(10,2)"

    def test_integer_to_number10(self):
        tm = ORACLE_DICT["type_map"]
        assert resolve_type("integer", tm, "oracle") == "NUMBER(10)"


class TestOracleDefaultResolution:
    def test_now_to_systimestamp(self):
        dm = ORACLE_DICT["default_map"]
        assert resolve_default("now()", dm, "oracle") == "SYSTIMESTAMP"

    def test_true_to_1(self):
        dm = ORACLE_DICT["default_map"]
        assert resolve_default("true", dm, "oracle") == "1"

    def test_uuid_to_sys_guid(self):
        dm = ORACLE_DICT["default_map"]
        assert resolve_default("uuid()", dm, "oracle") == "SYS_GUID()"


# ---------------------------------------------------------------------------
# compile_oracle
# ---------------------------------------------------------------------------

class TestCompileOracle:
    def test_result_counts(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        assert result.engine == "oracle"
        assert result.entity_count == 3
        assert result.index_count > 0
        assert result.fk_count == 1

    def test_file_count(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        names = [f["name"] for f in result.files]
        assert "001_schemas.sql" in names
        assert "002_tables.sql" in names
        assert "003_indexes.sql" in names
        assert "004_foreign_keys.sql" in names
        assert "005_comments.sql" in names

    def test_schema_idempotent(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "001_schemas.sql")
        assert "EXECUTE IMMEDIATE" in f["content"]
        assert "ORA-01920" in f["content"]  # user already exists
        assert "/" in f["content"]

    def test_table_idempotent(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "EXECUTE IMMEDIATE" in f["content"]
        assert "ORA-00955" in f["content"]  # name already used
        assert "BEGIN" in f["content"]

    def test_identity_in_ddl(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "GENERATED ALWAYS AS IDENTITY" in f["content"]

    def test_varchar2_in_ddl(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "VARCHAR2(50)" in f["content"]

    def test_timestamp_tz_in_ddl(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "TIMESTAMP WITH TIME ZONE" in f["content"]

    def test_default_systimestamp(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "SYSTIMESTAMP" in f["content"]

    def test_check_constraint(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "CHECK" in f["content"]
        assert "'ACTIVE'" in f["content"]

    def test_pk_constraint(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "002_tables.sql")
        assert "PRIMARY KEY" in f["content"]
        assert "pk_tenant" in f["content"]

    def test_fk_idempotent(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "004_foreign_keys.sql")
        assert "EXECUTE IMMEDIATE" in f["content"]
        assert "ORA-02275" in f["content"]  # constraint already exists
        assert "fk_agent_execution_agent_id" in f["content"]

    def test_index_idempotent(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "003_indexes.sql")
        assert "EXECUTE IMMEDIATE" in f["content"]
        assert "ORA-00955" in f["content"]

    def test_unique_index(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "003_indexes.sql")
        assert "UNIQUE INDEX" in f["content"]
        assert "ux_tenant_tenant_id" in f["content"]

    def test_comments_native(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary)
        f = next(f for f in result.files if f["name"] == "005_comments.sql")
        assert "COMMENT ON TABLE" in f["content"]
        assert "COMMENT ON COLUMN" in f["content"]

    def test_schema_filter(self, dictionary: DataDictionary):
        result = compile_oracle(dictionary, schema_filter="platform")
        assert result.entity_count == 1


# ---------------------------------------------------------------------------
# compile_and_write
# ---------------------------------------------------------------------------

class TestCompileAndWriteOracle:
    def test_writes_files(self, dictionary: DataDictionary, tmp_path: Path):
        result = compile_and_write(dictionary, engine="oracle", profile="essential", output_dir=tmp_path)
        assert result.engine == "oracle"
        files = list(tmp_path.glob("*.sql"))
        assert len(files) >= 4

    def test_no_security_ddl(self, dictionary: DataDictionary, tmp_path: Path):
        result = compile_and_write(dictionary, engine="oracle", profile="standard", output_dir=tmp_path)
        assert result.security_file_count == 0


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

class TestOracleCompileCLI:
    def test_compile_oracle_engine(self, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        out_dir = tmp_path / "sql"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "compile",
            "-e", "oracle",
            "-d", str(dict_file),
            "-o", str(out_dir),
        ])

        assert result.exit_code == 0
        assert "ORACLE" in result.output
        assert (out_dir / "002_tables.sql").exists()

    def test_compile_oracle_json_output(self, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        out_dir = tmp_path / "sql"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "compile",
            "-e", "oracle",
            "-d", str(dict_file),
            "-o", str(out_dir),
            "-j",
        ])

        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        json_start = next(i for i, l in enumerate(lines) if l.strip().startswith("{"))
        output = json.loads("\n".join(lines[json_start:]))
        assert output["engine"] == "oracle"
        assert output["entities"] == 3
