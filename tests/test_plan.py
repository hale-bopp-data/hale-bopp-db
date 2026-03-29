"""Tests for hb plan/apply — dictionary-driven schema planning."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from app.core.compile import DataDictionary
from app.core.plan import (
    _changes_hash,
    _file_hash,
    apply_plan,
    create_plan,
    dictionary_to_desired,
    load_plan,
    save_plan,
)
from app.models.schemas import (
    ChangeType,
    PlanMetadata,
    PlanResult,
    RiskLevel,
    SchemaChange,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal dictionary (same as test_compile)
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
                {"name": "id", "type": "auto", "nullable": False, "description": "PK"},
                {"name": "tenant_id", "type": "string(50)", "nullable": False, "description": "Business key"},
                {"name": "tenant_name", "type": "string(255)", "nullable": False},
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
    ],
    "relationships": [],
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
# dictionary_to_desired
# ---------------------------------------------------------------------------

class TestDictionaryToDesired:
    def test_converts_entities_to_tables(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary)
        assert "tables" in result
        assert "schemas" in result
        assert "platform.tenant" in result["tables"]
        assert "agent_mgmt.agent_registry" in result["tables"]

    def test_resolves_logical_types_to_pg(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary)
        tenant = result["tables"]["platform.tenant"]
        assert tenant["columns"]["id"]["type"] == "BIGSERIAL"
        assert tenant["columns"]["tenant_id"]["type"] == "VARCHAR(50)"
        assert tenant["columns"]["created_at"]["type"] == "TIMESTAMPTZ"

    def test_preserves_nullable(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary)
        tenant = result["tables"]["platform.tenant"]
        assert tenant["columns"]["id"]["nullable"] is False
        assert tenant["columns"]["tenant_name"]["nullable"] is False

    def test_preserves_primary_key(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary)
        tenant = result["tables"]["platform.tenant"]
        assert tenant["primary_key"] == ["id"]

        agent = result["tables"]["agent_mgmt.agent_registry"]
        assert agent["primary_key"] == ["agent_id"]

    def test_preserves_indexes(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary)
        tenant = result["tables"]["platform.tenant"]
        assert "ux_tenant_tenant_id" in tenant["indexes"]
        assert tenant["indexes"]["ux_tenant_tenant_id"]["unique"] is True

    def test_schema_filter(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary, schema_filter="platform")
        assert "platform.tenant" in result["tables"]
        assert "agent_mgmt.agent_registry" not in result["tables"]

    def test_nested_schemas_structure(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary)
        assert "platform" in result["schemas"]
        assert "agent_mgmt" in result["schemas"]
        assert "tenant" in result["schemas"]["platform"]["tables"]

    def test_preserves_comments(self, dictionary: DataDictionary):
        result = dictionary_to_desired(dictionary)
        tenant = result["tables"]["platform.tenant"]
        assert tenant["comment"] == "Root entity."
        assert tenant["column_comments"]["id"] == "PK"

    def test_fk_resolution(self):
        """FK references are converted to introspect-format FK dicts."""
        dd_with_fk = dict(MINIMAL_DICT)
        dd_with_fk = json.loads(json.dumps(MINIMAL_DICT))
        dd_with_fk["entities"].append({
            "id": "task",
            "name": "task",
            "schema": "agent_mgmt",
            "type": "FACT",
            "pk": {"columns": ["id"], "type": "auto"},
            "columns": [
                {"name": "id", "type": "auto", "nullable": False},
                {"name": "agent_id", "type": "string(100)", "nullable": False,
                 "fk": "agent_registry.agent_id"},
            ],
        })
        dd = DataDictionary.model_validate(dd_with_fk)
        result = dictionary_to_desired(dd)
        task = result["tables"]["agent_mgmt.task"]
        assert len(task["foreign_keys"]) == 1
        fk = task["foreign_keys"][0]
        assert fk["referred_table"] == "agent_registry"
        assert fk["referred_columns"] == ["agent_id"]


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

class TestHashing:
    def test_file_hash_deterministic(self, dict_file: Path):
        h1 = _file_hash(dict_file)
        h2 = _file_hash(dict_file)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_changes_hash_deterministic(self):
        changes = [
            SchemaChange(
                change_type=ChangeType.ADD_TABLE,
                object_name="test",
                sql_up="CREATE TABLE test (id INT);",
                sql_down="DROP TABLE test;",
            ),
        ]
        h1 = _changes_hash(changes)
        h2 = _changes_hash(changes)
        assert h1 == h2
        assert len(h1) == 64

    def test_changes_hash_varies_with_content(self):
        c1 = [SchemaChange(change_type=ChangeType.ADD_TABLE, object_name="a", sql_up="CREATE TABLE a;")]
        c2 = [SchemaChange(change_type=ChangeType.ADD_TABLE, object_name="b", sql_up="CREATE TABLE b;")]
        assert _changes_hash(c1) != _changes_hash(c2)

    def test_empty_changes_hash(self):
        h = _changes_hash([])
        assert len(h) == 64


# ---------------------------------------------------------------------------
# save_plan / load_plan
# ---------------------------------------------------------------------------

class TestPlanPersistence:
    def _make_plan(self) -> PlanResult:
        changes = [
            SchemaChange(
                change_type=ChangeType.ADD_TABLE,
                object_name="platform.tenant",
                sql_up='CREATE TABLE IF NOT EXISTS platform.tenant (id BIGSERIAL PRIMARY KEY);',
                sql_down='DROP TABLE IF EXISTS "platform.tenant";',
            ),
        ]
        return PlanResult(
            metadata=PlanMetadata(
                created_at="2026-03-25T10:00:00+00:00",
                dictionary_path="dict.json",
                dictionary_hash="abc123" * 10 + "abcd",
                connection="postgresql://user:***@localhost/testdb",
                engine="pg",
            ),
            changes=changes,
            risk_level=RiskLevel.LOW,
            plan_hash=_changes_hash(changes),
            rollback_sql='DROP TABLE IF EXISTS "platform.tenant";',
            summary={"add_table": 1},
        )

    def test_save_and_load_roundtrip(self, tmp_path: Path):
        plan = self._make_plan()
        path = save_plan(plan, tmp_path / "plan.json")
        assert path.exists()

        loaded = load_plan(path)
        assert loaded.metadata.engine == "pg"
        assert len(loaded.changes) == 1
        assert loaded.changes[0].change_type == ChangeType.ADD_TABLE
        assert loaded.risk_level == RiskLevel.LOW
        assert loaded.plan_hash == plan.plan_hash

    def test_load_detects_tampered_plan(self, tmp_path: Path):
        plan = self._make_plan()
        path = save_plan(plan, tmp_path / "plan.json")

        # Tamper with the plan file
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["changes"][0]["object_name"] = "hacked.table"
        path.write_text(json.dumps(raw), encoding="utf-8")

        with pytest.raises(ValueError, match="integrity check failed"):
            load_plan(path)

    def test_load_empty_changes_plan(self, tmp_path: Path):
        plan = PlanResult(
            metadata=PlanMetadata(
                created_at="2026-03-25T10:00:00+00:00",
                dictionary_path="dict.json",
                dictionary_hash="abc123" * 10 + "abcd",
                connection="postgresql://user:***@localhost/testdb",
                engine="pg",
            ),
            changes=[],
            risk_level=RiskLevel.LOW,
            plan_hash=_changes_hash([]),
            summary={},
        )
        path = save_plan(plan, tmp_path / "empty-plan.json")
        loaded = load_plan(path)
        assert len(loaded.changes) == 0

    def test_save_creates_parent_dirs(self, tmp_path: Path):
        plan = self._make_plan()
        path = save_plan(plan, tmp_path / "deep" / "nested" / "plan.json")
        assert path.exists()


# ---------------------------------------------------------------------------
# create_plan (with mocked introspect)
# ---------------------------------------------------------------------------

class TestCreatePlan:
    def _mock_introspect_empty(self, connection_string, schema=None):
        """Simulate an empty database."""
        return {"schemas": {}, "tables": {}}

    def _mock_introspect_with_tenant(self, connection_string, schema=None):
        """Simulate DB with tenant table already existing."""
        return {
            "schemas": {
                "platform": {
                    "tables": {
                        "tenant": {
                            "columns": {
                                "id": {"type": "BIGSERIAL", "nullable": False, "default": None},
                                "tenant_id": {"type": "VARCHAR(50)", "nullable": False, "default": None},
                                "tenant_name": {"type": "VARCHAR(255)", "nullable": False, "default": None},
                                "created_at": {"type": "TIMESTAMPTZ", "nullable": False, "default": None},
                            },
                            "indexes": {},
                            "primary_key": ["id"],
                            "foreign_keys": [],
                            "unique_constraints": [],
                            "check_constraints": [],
                            "comment": None,
                            "column_comments": {},
                        }
                    }
                }
            },
            "tables": {
                "platform.tenant": {
                    "columns": {
                        "id": {"type": "BIGSERIAL", "nullable": False, "default": None},
                        "tenant_id": {"type": "VARCHAR(50)", "nullable": False, "default": None},
                        "tenant_name": {"type": "VARCHAR(255)", "nullable": False, "default": None},
                        "created_at": {"type": "TIMESTAMPTZ", "nullable": False, "default": None},
                    },
                    "indexes": {},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                    "unique_constraints": [],
                    "check_constraints": [],
                    "comment": None,
                    "column_comments": {},
                }
            },
        }

    @patch("app.core.plan.introspect_schema")
    def test_plan_against_empty_db(self, mock_introspect, dict_file: Path):
        mock_introspect.side_effect = self._mock_introspect_empty
        plan = create_plan("postgresql://u:p@localhost/db", dict_file)

        assert len(plan.changes) > 0
        assert plan.risk_level == RiskLevel.LOW  # only ADD operations
        assert plan.metadata.engine == "pg"
        assert plan.plan_hash
        assert plan.metadata.dictionary_hash
        # All changes should be ADD_TABLE (new tables on empty DB)
        for c in plan.changes:
            assert c.change_type == ChangeType.ADD_TABLE

    @patch("app.core.plan.introspect_schema")
    def test_plan_no_changes_when_in_sync(self, mock_introspect, dict_file: Path):
        mock_introspect.side_effect = self._mock_introspect_with_tenant

        # Dictionary with only tenant entity (matching the mock)
        single_entity_dict = json.loads(json.dumps(MINIMAL_DICT))
        single_entity_dict["entities"] = [single_entity_dict["entities"][0]]
        single_file = dict_file.parent / "single.json"
        single_file.write_text(json.dumps(single_entity_dict), encoding="utf-8")

        plan = create_plan(
            "postgresql://u:p@localhost/db",
            single_file,
            schema_filter="platform",
        )
        assert len(plan.changes) == 0
        assert plan.risk_level == RiskLevel.LOW

    @patch("app.core.plan.introspect_schema")
    def test_plan_summary_counts(self, mock_introspect, dict_file: Path):
        mock_introspect.side_effect = self._mock_introspect_empty
        plan = create_plan("postgresql://u:p@localhost/db", dict_file)

        assert "add_table" in plan.summary
        assert plan.summary["add_table"] == 2  # tenant + agent_registry

    @patch("app.core.plan.introspect_schema")
    def test_plan_rollback_sql_present(self, mock_introspect, dict_file: Path):
        mock_introspect.side_effect = self._mock_introspect_empty
        plan = create_plan("postgresql://u:p@localhost/db", dict_file)

        assert plan.rollback_sql
        assert "DROP TABLE" in plan.rollback_sql

    @patch("app.core.plan.introspect_schema")
    def test_plan_sanitizes_connection(self, mock_introspect, dict_file: Path):
        mock_introspect.side_effect = self._mock_introspect_empty
        plan = create_plan("postgresql://admin:s3cret@db.example.com/prod", dict_file)

        assert "s3cret" not in plan.metadata.connection
        assert "***" in plan.metadata.connection

    @patch("app.core.plan.introspect_schema")
    def test_plan_with_schema_filter(self, mock_introspect, dict_file: Path):
        mock_introspect.side_effect = self._mock_introspect_empty
        plan = create_plan(
            "postgresql://u:p@localhost/db",
            dict_file,
            schema_filter="platform",
        )
        assert plan.metadata.schema_filter == "platform"
        # Only platform entities
        for c in plan.changes:
            assert "platform" in c.object_name


# ---------------------------------------------------------------------------
# apply_plan (with mocked deploy)
# ---------------------------------------------------------------------------

class TestApplyPlan:
    def _make_plan_with_changes(self) -> PlanResult:
        changes = [
            SchemaChange(
                change_type=ChangeType.ADD_TABLE,
                object_name="platform.tenant",
                sql_up='CREATE TABLE IF NOT EXISTS platform.tenant (id BIGSERIAL PRIMARY KEY);',
                sql_down='DROP TABLE IF EXISTS "platform.tenant";',
            ),
        ]
        return PlanResult(
            metadata=PlanMetadata(
                created_at="2026-03-25T10:00:00+00:00",
                dictionary_path="dict.json",
                dictionary_hash="a" * 64,
                connection="postgresql://user:***@localhost/testdb",
                engine="pg",
            ),
            changes=changes,
            risk_level=RiskLevel.LOW,
            plan_hash=_changes_hash(changes),
            rollback_sql='DROP TABLE IF EXISTS "platform.tenant";',
            summary={"add_table": 1},
        )

    @patch("app.core.plan.deploy_changes")
    def test_apply_dry_run(self, mock_deploy):
        plan = self._make_plan_with_changes()
        mock_deploy.return_value = (plan.changes, plan.rollback_sql)

        applied, rollback = apply_plan("postgresql://u:p@localhost/db", plan, dry_run=True)

        mock_deploy.assert_called_once_with(
            connection_string="postgresql://u:p@localhost/db",
            changes=plan.changes,
            dry_run=True,
        )
        assert len(applied) == 1

    @patch("app.core.plan.deploy_changes")
    def test_apply_execute(self, mock_deploy):
        plan = self._make_plan_with_changes()
        mock_deploy.return_value = (plan.changes, plan.rollback_sql)

        applied, rollback = apply_plan("postgresql://u:p@localhost/db", plan, dry_run=False)

        mock_deploy.assert_called_once_with(
            connection_string="postgresql://u:p@localhost/db",
            changes=plan.changes,
            dry_run=False,
        )

    @patch("app.core.plan.deploy_changes")
    def test_apply_propagates_errors(self, mock_deploy):
        plan = self._make_plan_with_changes()
        mock_deploy.side_effect = RuntimeError("Connection refused")

        with pytest.raises(RuntimeError, match="Connection refused"):
            apply_plan("postgresql://u:p@localhost/db", plan, dry_run=False)


# ---------------------------------------------------------------------------
# CLI integration (Click runner)
# ---------------------------------------------------------------------------

class TestPlanCLI:
    @patch("app.core.plan.introspect_schema")
    def test_plan_cli_json_output(self, mock_introspect, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        mock_introspect.return_value = {"schemas": {}, "tables": {}}

        runner = CliRunner()
        plan_out = tmp_path / "plan.json"
        result = runner.invoke(cli, [
            "plan",
            "-c", "postgresql://u:p@localhost/db",
            "-d", str(dict_file),
            "-o", str(plan_out),
            "-j",
        ])

        # Exit code 1 = changes found (expected for empty DB)
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "changes" in output
        assert "risk_level" in output
        assert output["risk_level"] == "low"

    @patch("app.core.plan.introspect_schema")
    def test_plan_cli_human_output(self, mock_introspect, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        mock_introspect.return_value = {"schemas": {}, "tables": {}}

        runner = CliRunner()
        plan_out = tmp_path / "plan.json"
        result = runner.invoke(cli, [
            "plan",
            "-c", "postgresql://u:p@localhost/db",
            "-d", str(dict_file),
            "-o", str(plan_out),
        ])

        assert result.exit_code == 1
        assert "Plan saved" in result.output
        assert "Plan hash" in result.output

    @patch("app.core.plan.deploy_changes")
    def test_apply_cli_dry_run(self, mock_deploy, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        # Create a plan file first
        changes = [
            SchemaChange(
                change_type=ChangeType.ADD_TABLE,
                object_name="platform.tenant",
                sql_up="CREATE TABLE platform.tenant (id INT);",
                sql_down="DROP TABLE platform.tenant;",
            )
        ]
        plan = PlanResult(
            metadata=PlanMetadata(
                created_at="2026-03-25T10:00:00+00:00",
                dictionary_path="dict.json",
                dictionary_hash="a" * 64,
                connection="postgresql://user:***@localhost/testdb",
                engine="pg",
            ),
            changes=changes,
            risk_level=RiskLevel.LOW,
            plan_hash=_changes_hash(changes),
            rollback_sql="DROP TABLE platform.tenant;",
            summary={"add_table": 1},
        )
        plan_file = tmp_path / "plan.json"
        save_plan(plan, plan_file)

        mock_deploy.return_value = (changes, "DROP TABLE platform.tenant;")

        runner = CliRunner()
        result = runner.invoke(cli, [
            "apply",
            "-c", "postgresql://u:p@localhost/db",
            "--plan", str(plan_file),
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        mock_deploy.assert_called_once()
        assert mock_deploy.call_args.kwargs["dry_run"] is True

    @patch("app.core.plan.deploy_changes")
    def test_apply_cli_execute(self, mock_deploy, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        changes = [
            SchemaChange(
                change_type=ChangeType.ADD_TABLE,
                object_name="platform.tenant",
                sql_up="CREATE TABLE platform.tenant (id INT);",
                sql_down="DROP TABLE platform.tenant;",
            )
        ]
        plan = PlanResult(
            metadata=PlanMetadata(
                created_at="2026-03-25T10:00:00+00:00",
                dictionary_path="dict.json",
                dictionary_hash="a" * 64,
                connection="postgresql://user:***@localhost/testdb",
                engine="pg",
            ),
            changes=changes,
            risk_level=RiskLevel.LOW,
            plan_hash=_changes_hash(changes),
            rollback_sql="DROP TABLE platform.tenant;",
            summary={"add_table": 1},
        )
        plan_file = tmp_path / "plan.json"
        save_plan(plan, plan_file)

        mock_deploy.return_value = (changes, "DROP TABLE platform.tenant;")

        runner = CliRunner()
        result = runner.invoke(cli, [
            "apply",
            "-c", "postgresql://u:p@localhost/db",
            "--plan", str(plan_file),
            "--execute",
        ])

        assert result.exit_code == 0
        assert "Applied 1 change(s)" in result.output
        assert mock_deploy.call_args.kwargs["dry_run"] is False
