"""Tests for DB-HALE-BOPP CLI commands."""

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from app.cli import cli
from app.version import __version__


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def desired_schema(tmp_path):
    schema = {
        "tables": {
            "users": {
                "columns": {
                    "id": {"type": "INTEGER", "nullable": False},
                    "email": {"type": "VARCHAR(255)", "nullable": True},
                },
                "indexes": {},
                "primary_key": ["id"],
            }
        }
    }
    path = tmp_path / "desired.json"
    path.write_text(json.dumps(schema))
    return str(path)


def test_version(runner):
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


@patch("app.cli.introspect_schema")
def test_diff_no_changes(mock_introspect, runner, desired_schema):
    mock_introspect.return_value = json.loads(open(desired_schema).read())
    result = runner.invoke(cli, ["diff", "-c", "postgresql://fake@localhost/db", "-d", desired_schema])
    assert result.exit_code == 0
    assert "No changes" in result.output


@patch("app.cli.introspect_schema")
def test_diff_with_changes(mock_introspect, runner, desired_schema):
    mock_introspect.return_value = {"tables": {}}  # empty DB
    result = runner.invoke(cli, ["diff", "-c", "postgresql://fake@localhost/db", "-d", desired_schema])
    assert result.exit_code == 1
    assert "add_table" in result.output


@patch("app.cli.introspect_schema")
def test_diff_json_output(mock_introspect, runner, desired_schema):
    mock_introspect.return_value = {"tables": {}}
    result = runner.invoke(cli, ["diff", "-c", "postgresql://fake@localhost/db", "-d", desired_schema, "-j"])
    # JSON mode suppresses "Connecting..." so output is pure JSON
    data = json.loads(result.output)
    assert "changes" in data
    assert data["risk_level"] == "low"
    assert len(data["changes"]) == 1


@patch("app.cli.deploy_changes")
def test_deploy_dry_run(mock_deploy, runner, tmp_path):
    changes_file = tmp_path / "changes.json"
    changes_file.write_text(json.dumps({"changes": []}))
    mock_deploy.return_value = ([], "-- rollback")

    result = runner.invoke(cli, ["deploy", "-c", "postgresql://fake@localhost/db", "--changes", str(changes_file)])
    assert result.exit_code == 0
    assert "DRY RUN" in result.output


@patch("app.cli.introspect_schema")
def test_drift_no_drift(mock_introspect, runner, desired_schema):
    mock_introspect.return_value = json.loads(open(desired_schema).read())
    result = runner.invoke(cli, ["drift", "-c", "postgresql://fake@localhost/db", "-b", desired_schema])
    assert result.exit_code == 0
    assert "No drift" in result.output


@patch("app.cli.introspect_schema")
def test_drift_detected(mock_introspect, runner, desired_schema):
    # Actual has extra column not in baseline
    actual = json.loads(open(desired_schema).read())
    actual["tables"]["users"]["columns"]["extra_col"] = {"type": "TEXT"}
    mock_introspect.return_value = actual
    result = runner.invoke(cli, ["drift", "-c", "postgresql://fake@localhost/db", "-b", desired_schema])
    assert result.exit_code == 1
    assert "DRIFT DETECTED" in result.output


@patch("app.cli.introspect_schema")
def test_snapshot(mock_introspect, runner, tmp_path):
    mock_introspect.return_value = {
        "tables": {"orders": {"columns": {"id": {"type": "INTEGER"}}, "indexes": {}, "primary_key": ["id"]}}
    }
    out_file = tmp_path / "baseline.json"
    result = runner.invoke(cli, ["snapshot", "-c", "postgresql://fake@localhost/db", "-o", str(out_file)])
    assert result.exit_code == 0
    assert "Snapshot saved" in result.output
    assert out_file.exists()

    data = json.loads(out_file.read_text())
    assert "orders" in data["tables"]


def test_connection_sanitized(runner):
    """Verify password is hidden in output."""
    from app.cli import _sanitize_conn
    assert "***" in _sanitize_conn("postgresql://user:secret@localhost/db")
    assert "secret" not in _sanitize_conn("postgresql://user:secret@localhost/db")
