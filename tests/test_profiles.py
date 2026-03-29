"""Tests for hb environments — hb-profiles.yml multi-env config."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from app.core.profiles import (
    Environment,
    ProfileConfig,
    find_profiles,
    load_profiles,
    resolve_env,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_PROFILES = {
    "default_env": "dev",
    "environments": {
        "dev": {
            "connection": "postgresql://dev:dev@localhost:5432/halebopp_dev",
            "engine": "pg",
            "profile": "essential",
            "description": "Local development",
        },
        "staging": {
            "connection": "postgresql://app:secret@staging.db:5432/halebopp",
            "engine": "pg",
            "profile": "standard",
        },
        "prod": {
            "connection": "postgresql://app:prod_secret@prod.db:5432/halebopp",
            "engine": "pg",
            "profile": "enterprise",
            "schema_filter": "platform",
        },
    },
}


@pytest.fixture
def profiles_file(tmp_path: Path) -> Path:
    p = tmp_path / "hb-profiles.yml"
    p.write_text(yaml.dump(SAMPLE_PROFILES), encoding="utf-8")
    return p


@pytest.fixture
def dict_file(tmp_path: Path) -> Path:
    """Minimal dictionary for CLI tests."""
    minimal = {
        "type_map": {"auto": {"pg": "BIGSERIAL"}, "string(n)": {"pg": "VARCHAR({n})"},
                     "timestamp": {"pg": "TIMESTAMPTZ"}},
        "default_map": {"now()": {"pg": "NOW()"}},
        "schemas": [{"name": "platform"}],
        "entities": [{
            "name": "tenant", "schema": "platform", "type": "DIM",
            "pk": {"columns": ["id"]},
            "columns": [
                {"name": "id", "type": "auto", "nullable": False},
                {"name": "name", "type": "string(100)", "nullable": False},
            ],
        }],
    }
    p = tmp_path / "dict.json"
    p.write_text(json.dumps(minimal), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# ProfileConfig model validation
# ---------------------------------------------------------------------------

class TestProfileConfig:
    def test_load_valid(self, profiles_file: Path):
        config = load_profiles(profiles_file)
        assert "dev" in config.environments
        assert "staging" in config.environments
        assert "prod" in config.environments
        assert config.default_env == "dev"

    def test_environment_fields(self, profiles_file: Path):
        config = load_profiles(profiles_file)
        dev = config.environments["dev"]
        assert dev.engine == "pg"
        assert dev.profile == "essential"
        assert "localhost" in dev.connection

        prod = config.environments["prod"]
        assert prod.profile == "enterprise"
        assert prod.schema_filter == "platform"

    def test_invalid_engine_rejected(self, tmp_path: Path):
        bad = {
            "environments": {
                "dev": {"connection": "pg://localhost/db", "engine": "mysql"},
            }
        }
        p = tmp_path / "hb-profiles.yml"
        p.write_text(yaml.dump(bad), encoding="utf-8")
        with pytest.raises(Exception):  # ValidationError
            load_profiles(p)

    def test_invalid_profile_rejected(self, tmp_path: Path):
        bad = {
            "environments": {
                "dev": {"connection": "pg://localhost/db", "profile": "maximum"},
            }
        }
        p = tmp_path / "hb-profiles.yml"
        p.write_text(yaml.dump(bad), encoding="utf-8")
        with pytest.raises(Exception):
            load_profiles(p)

    def test_empty_environments_rejected(self, tmp_path: Path):
        bad = {"environments": {}}
        p = tmp_path / "hb-profiles.yml"
        p.write_text(yaml.dump(bad), encoding="utf-8")
        with pytest.raises(Exception):
            load_profiles(p)

    def test_non_dict_yaml_rejected(self, tmp_path: Path):
        p = tmp_path / "hb-profiles.yml"
        p.write_text("- just a list", encoding="utf-8")
        with pytest.raises(ValueError, match="expected YAML mapping"):
            load_profiles(p)

    def test_defaults_applied(self, tmp_path: Path):
        minimal = {
            "environments": {
                "dev": {"connection": "pg://localhost/db"},
            }
        }
        p = tmp_path / "hb-profiles.yml"
        p.write_text(yaml.dump(minimal), encoding="utf-8")
        config = load_profiles(p)
        dev = config.environments["dev"]
        assert dev.engine == "pg"
        assert dev.profile == "essential"
        assert dev.schema_filter is None


# ---------------------------------------------------------------------------
# find_profiles
# ---------------------------------------------------------------------------

class TestFindProfiles:
    def test_finds_in_current_dir(self, tmp_path: Path):
        (tmp_path / "hb-profiles.yml").write_text(yaml.dump(SAMPLE_PROFILES), encoding="utf-8")
        result = find_profiles(tmp_path)
        assert result is not None
        assert result.name == "hb-profiles.yml"

    def test_finds_in_parent_dir(self, tmp_path: Path):
        (tmp_path / "hb-profiles.yml").write_text(yaml.dump(SAMPLE_PROFILES), encoding="utf-8")
        child = tmp_path / "subdir"
        child.mkdir()
        result = find_profiles(child)
        assert result is not None

    def test_returns_none_if_not_found(self, tmp_path: Path):
        result = find_profiles(tmp_path)
        assert result is None


# ---------------------------------------------------------------------------
# resolve_env
# ---------------------------------------------------------------------------

class TestResolveEnv:
    def test_resolve_existing_env(self, profiles_file: Path):
        env = resolve_env("dev", profiles_path=profiles_file)
        assert env is not None
        assert env.engine == "pg"
        assert env.profile == "essential"

    def test_resolve_prod(self, profiles_file: Path):
        env = resolve_env("prod", profiles_path=profiles_file)
        assert env.profile == "enterprise"
        assert env.schema_filter == "platform"

    def test_none_returns_none(self, profiles_file: Path):
        result = resolve_env(None, profiles_path=profiles_file)
        assert result is None

    def test_missing_env_raises(self, profiles_file: Path):
        with pytest.raises(ValueError, match="not found"):
            resolve_env("nonexistent", profiles_path=profiles_file)

    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            resolve_env("dev", profiles_path=tmp_path / "nope.yml")


# ---------------------------------------------------------------------------
# CLI --env integration
# ---------------------------------------------------------------------------

class TestCLIEnvFlag:
    @patch("app.core.plan.introspect_schema")
    def test_plan_with_env(self, mock_introspect, profiles_file: Path, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        mock_introspect.return_value = {"schemas": {}, "tables": {}}

        # Patch find_profiles to return our fixture
        with patch("app.core.profiles.find_profiles", return_value=profiles_file):
            runner = CliRunner()
            result = runner.invoke(cli, [
                "plan",
                "--env", "dev",
                "-d", str(dict_file),
                "-o", str(tmp_path / "plan.json"),
                "-j",
            ])

        # Should succeed (exit 1 = changes found, which is expected)
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "changes" in output

    @patch("app.core.drift_detect.introspect_schema")
    def test_drift_with_env(self, mock_introspect, profiles_file: Path, dict_file: Path):
        from click.testing import CliRunner
        from app.cli import cli

        mock_introspect.return_value = {"schemas": {}, "tables": {}}

        with patch("app.core.profiles.find_profiles", return_value=profiles_file):
            runner = CliRunner()
            result = runner.invoke(cli, [
                "drift",
                "--env", "staging",
                "-d", str(dict_file),
                "-j",
            ])

        assert result.exit_code == 1  # drift detected (empty DB vs dictionary)
        output = json.loads(result.output)
        assert output["has_drift"] is True
        assert output["profile"] == "standard"

    def test_compile_with_env(self, profiles_file: Path, dict_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        with patch("app.core.profiles.find_profiles", return_value=profiles_file):
            runner = CliRunner()
            out_dir = tmp_path / "sql"
            result = runner.invoke(cli, [
                "compile",
                "--env", "prod",
                "-d", str(dict_file),
                "-o", str(out_dir),
            ])

        assert result.exit_code == 0
        # Should use enterprise profile from prod env
        assert "enterprise" in result.output.lower()

    @patch("app.core.plan.deploy_changes")
    def test_apply_with_env(self, mock_deploy, profiles_file: Path, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli
        from app.core.plan import _changes_hash, save_plan
        from app.models.schemas import (
            ChangeType, PlanMetadata, PlanResult, RiskLevel, SchemaChange,
        )

        changes = [SchemaChange(
            change_type=ChangeType.ADD_TABLE,
            object_name="platform.tenant",
            sql_up="CREATE TABLE platform.tenant (id INT);",
            sql_down="DROP TABLE platform.tenant;",
        )]
        plan = PlanResult(
            metadata=PlanMetadata(
                created_at="2026-03-25T10:00:00+00:00",
                dictionary_path="dict.json",
                dictionary_hash="a" * 64,
                connection="postgresql://***@localhost/db",
                engine="pg",
            ),
            changes=changes,
            risk_level=RiskLevel.LOW,
            plan_hash=_changes_hash(changes),
            summary={"add_table": 1},
        )
        plan_file = tmp_path / "plan.json"
        save_plan(plan, plan_file)

        mock_deploy.return_value = (changes, "DROP TABLE platform.tenant;")

        with patch("app.core.profiles.find_profiles", return_value=profiles_file):
            runner = CliRunner()
            result = runner.invoke(cli, [
                "apply",
                "--env", "dev",
                "--plan", str(plan_file),
            ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    def test_env_missing_profiles_file_error(self, tmp_path: Path, dict_file: Path):
        from click.testing import CliRunner
        from app.cli import cli

        with patch("app.core.profiles.find_profiles", return_value=None):
            runner = CliRunner()
            result = runner.invoke(cli, [
                "plan",
                "--env", "dev",
                "-d", str(dict_file),
                "-o", str(tmp_path / "plan.json"),
            ])

        assert result.exit_code != 0

    def test_backward_compat_without_env(self, dict_file: Path, tmp_path: Path):
        """Commands still work with explicit --connection flag (no --env)."""
        from click.testing import CliRunner
        from app.cli import cli

        runner = CliRunner()
        out_dir = tmp_path / "sql"
        result = runner.invoke(cli, [
            "compile",
            "-e", "pg",
            "-p", "essential",
            "-d", str(dict_file),
            "-o", str(out_dir),
        ])
        assert result.exit_code == 0
