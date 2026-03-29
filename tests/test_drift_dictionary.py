"""Tests for hb drift — dictionary-based drift detection."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from app.core.compile import DataDictionary
from app.core.drift_detect import detect_drift, detect_drift_from_schemas
from app.core.plan import dictionary_to_desired
from app.models.schemas import DriftReport, DriftType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MINIMAL_DICT = {
    "type_map": {
        "uuid": {"pg": "TEXT"},
        "string": {"pg": "TEXT"},
        "string(n)": {"pg": "VARCHAR({n})"},
        "integer": {"pg": "INTEGER"},
        "auto": {"pg": "BIGSERIAL"},
        "boolean": {"pg": "BOOLEAN"},
        "timestamp": {"pg": "TIMESTAMPTZ"},
        "decimal(p,s)": {"pg": "NUMERIC({p},{s})"},
        "json": {"pg": "JSONB"},
    },
    "default_map": {
        "now()": {"pg": "NOW()"},
        "true": {"pg": "TRUE"},
    },
    "schemas": [{"name": "platform"}],
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
                {"name": "id", "type": "auto", "nullable": False},
                {"name": "tenant_id", "type": "string(50)", "nullable": False},
                {"name": "tenant_name", "type": "string(255)", "nullable": False},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
            ],
            "indexes": [
                {"name": "ux_tenant_tenant_id", "columns": ["tenant_id"], "unique": True},
            ],
        },
    ],
}

DICT_WITH_RLS = {
    **MINIMAL_DICT,
    "entities": [
        {
            **MINIMAL_DICT["entities"][0],
            "multi_tenant": True,
            "security": {"rls": True},
        },
    ],
}

DICT_WITH_MASKING = {
    **MINIMAL_DICT,
    "entities": [
        {
            **MINIMAL_DICT["entities"][0],
            "security": {"masking": {"tenant_name": "full"}},
        },
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


@pytest.fixture
def dict_rls_file(tmp_path: Path) -> Path:
    p = tmp_path / "dict_rls.json"
    p.write_text(json.dumps(DICT_WITH_RLS), encoding="utf-8")
    return p


@pytest.fixture
def dict_masking_file(tmp_path: Path) -> Path:
    p = tmp_path / "dict_masking.json"
    p.write_text(json.dumps(DICT_WITH_MASKING), encoding="utf-8")
    return p


def _make_actual_matching(dictionary: DataDictionary) -> dict:
    """Create an actual schema that matches the desired exactly."""
    return dictionary_to_desired(dictionary)


def _make_actual_with_extra_column(dictionary: DataDictionary) -> dict:
    """DB has an extra column not in dictionary."""
    actual = dictionary_to_desired(dictionary)
    actual["tables"]["platform.tenant"]["columns"]["rogue_col"] = {
        "type": "TEXT", "nullable": True, "default": None,
    }
    return actual


def _make_actual_missing_column(dictionary: DataDictionary) -> dict:
    """DB is missing a column from dictionary."""
    actual = dictionary_to_desired(dictionary)
    del actual["tables"]["platform.tenant"]["columns"]["tenant_name"]
    return actual


def _make_actual_type_mismatch(dictionary: DataDictionary) -> dict:
    """DB has different type for a column."""
    actual = dictionary_to_desired(dictionary)
    actual["tables"]["platform.tenant"]["columns"]["tenant_id"]["type"] = "TEXT"
    return actual


def _make_actual_missing_index(dictionary: DataDictionary) -> dict:
    """DB is missing an index."""
    actual = dictionary_to_desired(dictionary)
    del actual["tables"]["platform.tenant"]["indexes"]["ux_tenant_tenant_id"]
    return actual


def _make_actual_extra_index(dictionary: DataDictionary) -> dict:
    """DB has an extra index not in dictionary."""
    actual = dictionary_to_desired(dictionary)
    actual["tables"]["platform.tenant"]["indexes"]["ix_rogue"] = {
        "columns": ["created_at"], "unique": False,
    }
    return actual


# ---------------------------------------------------------------------------
# No drift
# ---------------------------------------------------------------------------

class TestNoDrift:
    def test_in_sync(self, dictionary: DataDictionary):
        actual = _make_actual_matching(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert not report.has_drift
        assert len(report.items) == 0

    def test_empty_summary(self, dictionary: DataDictionary):
        actual = _make_actual_matching(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.summary == {}


# ---------------------------------------------------------------------------
# Table drift
# ---------------------------------------------------------------------------

class TestTableDrift:
    def test_extra_table(self, dictionary: DataDictionary):
        actual = _make_actual_matching(dictionary)
        actual["tables"]["platform.rogue_table"] = {"columns": {}, "indexes": {}}
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        extras = [i for i in report.items if i.drift_type == DriftType.EXTRA_TABLE]
        assert len(extras) == 1
        assert "rogue_table" in extras[0].object_name

    def test_missing_table(self, dictionary: DataDictionary):
        actual = {"tables": {}, "schemas": {}}
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        missing = [i for i in report.items if i.drift_type == DriftType.MISSING_TABLE]
        assert len(missing) == 1


# ---------------------------------------------------------------------------
# Column drift
# ---------------------------------------------------------------------------

class TestColumnDrift:
    def test_extra_column(self, dictionary: DataDictionary):
        actual = _make_actual_with_extra_column(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        extras = [i for i in report.items if i.drift_type == DriftType.EXTRA_COLUMN]
        assert len(extras) == 1
        assert "rogue_col" in extras[0].object_name

    def test_missing_column(self, dictionary: DataDictionary):
        actual = _make_actual_missing_column(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        missing = [i for i in report.items if i.drift_type == DriftType.MISSING_COLUMN]
        assert len(missing) == 1
        assert "tenant_name" in missing[0].object_name

    def test_type_mismatch(self, dictionary: DataDictionary):
        actual = _make_actual_type_mismatch(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        mismatches = [i for i in report.items if i.drift_type == DriftType.TYPE_MISMATCH]
        assert len(mismatches) == 1
        assert mismatches[0].details["actual"] == "TEXT"
        assert mismatches[0].details["desired"] == "VARCHAR(50)"


# ---------------------------------------------------------------------------
# Index drift
# ---------------------------------------------------------------------------

class TestIndexDrift:
    def test_missing_index(self, dictionary: DataDictionary):
        actual = _make_actual_missing_index(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        missing = [i for i in report.items if i.drift_type == DriftType.MISSING_INDEX]
        assert len(missing) == 1
        assert "ux_tenant_tenant_id" in missing[0].object_name

    def test_extra_index(self, dictionary: DataDictionary):
        actual = _make_actual_extra_index(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        extras = [i for i in report.items if i.drift_type == DriftType.EXTRA_INDEX]
        assert len(extras) == 1
        assert "ix_rogue" in extras[0].object_name


# ---------------------------------------------------------------------------
# Security drift
# ---------------------------------------------------------------------------

class TestSecurityDrift:
    def test_missing_rls_standard(self):
        dd = DataDictionary.model_validate(DICT_WITH_RLS)
        actual = _make_actual_matching(dd)
        # Ensure schemas structure for RLS check
        actual["schemas"] = {
            "platform": {
                "tables": {
                    "tenant": actual["tables"]["platform.tenant"]
                }
            }
        }
        desired = dictionary_to_desired(dd)
        report = detect_drift_from_schemas(
            actual, desired, dictionary=dd, profile="standard",
        )
        rls = [i for i in report.items if i.drift_type == DriftType.MISSING_RLS]
        assert len(rls) == 1
        assert "ENABLE ROW LEVEL SECURITY" in rls[0].suggested_action

    def test_no_rls_check_essential(self):
        dd = DataDictionary.model_validate(DICT_WITH_RLS)
        actual = _make_actual_matching(dd)
        desired = dictionary_to_desired(dd)
        report = detect_drift_from_schemas(
            actual, desired, dictionary=dd, profile="essential",
        )
        rls = [i for i in report.items if i.drift_type == DriftType.MISSING_RLS]
        assert len(rls) == 0

    def test_missing_masking_enterprise(self):
        dd = DataDictionary.model_validate(DICT_WITH_MASKING)
        actual = _make_actual_matching(dd)
        desired = dictionary_to_desired(dd)
        report = detect_drift_from_schemas(
            actual, desired, dictionary=dd, profile="enterprise",
        )
        masking = [i for i in report.items if i.drift_type == DriftType.MISSING_MASKING]
        assert len(masking) == 1
        assert "v_tenant_masked" in masking[0].object_name

    def test_no_masking_check_standard(self):
        dd = DataDictionary.model_validate(DICT_WITH_MASKING)
        actual = _make_actual_matching(dd)
        desired = dictionary_to_desired(dd)
        report = detect_drift_from_schemas(
            actual, desired, dictionary=dd, profile="standard",
        )
        masking = [i for i in report.items if i.drift_type == DriftType.MISSING_MASKING]
        assert len(masking) == 0


# ---------------------------------------------------------------------------
# Suggested actions
# ---------------------------------------------------------------------------

class TestSuggestedActions:
    def test_extra_column_suggests_drop(self, dictionary: DataDictionary):
        actual = _make_actual_with_extra_column(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        extras = [i for i in report.items if i.drift_type == DriftType.EXTRA_COLUMN]
        assert "DROP COLUMN" in extras[0].suggested_action or "add to dictionary" in extras[0].suggested_action

    def test_missing_column_suggests_apply(self, dictionary: DataDictionary):
        actual = _make_actual_missing_column(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        missing = [i for i in report.items if i.drift_type == DriftType.MISSING_COLUMN]
        assert "hb apply" in missing[0].suggested_action

    def test_type_mismatch_suggests_alter(self, dictionary: DataDictionary):
        actual = _make_actual_type_mismatch(dictionary)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        mismatches = [i for i in report.items if i.drift_type == DriftType.TYPE_MISMATCH]
        assert "ALTER" in mismatches[0].suggested_action


# ---------------------------------------------------------------------------
# Summary counts
# ---------------------------------------------------------------------------

class TestSummaryCounts:
    def test_counts_match_items(self, dictionary: DataDictionary):
        actual = _make_actual_with_extra_column(dictionary)
        actual = _make_actual_missing_index(dictionary)
        # Combine: missing index + extra nothing (clean actual)
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        total_from_summary = sum(report.summary.values())
        assert total_from_summary == len(report.items)

    def test_multiple_drift_types(self, dictionary: DataDictionary):
        actual = _make_actual_with_extra_column(dictionary)
        # Also remove an index
        del actual["tables"]["platform.tenant"]["indexes"]["ux_tenant_tenant_id"]
        desired = dictionary_to_desired(dictionary)
        report = detect_drift_from_schemas(actual, desired)
        assert report.has_drift
        assert len(report.summary) >= 2  # at least extra_column + missing_index


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

class TestDriftCLI:
    @patch("app.core.drift_detect.introspect_schema")
    def test_drift_cli_dictionary_mode_json(self, mock_introspect, dict_file: Path):
        from click.testing import CliRunner
        from app.cli import cli

        # Return actual that matches desired → no drift
        dd = DataDictionary.model_validate(MINIMAL_DICT)
        mock_introspect.return_value = dictionary_to_desired(dd)

        runner = CliRunner()
        result = runner.invoke(cli, [
            "drift",
            "-c", "postgresql://u:p@localhost/db",
            "-d", str(dict_file),
            "-j",
        ])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["has_drift"] is False

    @patch("app.core.drift_detect.introspect_schema")
    def test_drift_cli_detects_extra_column(self, mock_introspect, dict_file: Path):
        from click.testing import CliRunner
        from app.cli import cli

        dd = DataDictionary.model_validate(MINIMAL_DICT)
        actual = dictionary_to_desired(dd)
        actual["tables"]["platform.tenant"]["columns"]["rogue"] = {
            "type": "TEXT", "nullable": True, "default": None,
        }
        mock_introspect.return_value = actual

        runner = CliRunner()
        result = runner.invoke(cli, [
            "drift",
            "-c", "postgresql://u:p@localhost/db",
            "-d", str(dict_file),
            "-j",
        ])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["has_drift"] is True
        assert any(i["drift_type"] == "extra_column" for i in output["items"])

    @patch("app.core.drift_detect.introspect_schema")
    def test_drift_cli_human_output(self, mock_introspect, dict_file: Path):
        from click.testing import CliRunner
        from app.cli import cli

        dd = DataDictionary.model_validate(MINIMAL_DICT)
        actual = dictionary_to_desired(dd)
        del actual["tables"]["platform.tenant"]["columns"]["tenant_name"]
        mock_introspect.return_value = actual

        runner = CliRunner()
        result = runner.invoke(cli, [
            "drift",
            "-c", "postgresql://u:p@localhost/db",
            "-d", str(dict_file),
        ])

        assert result.exit_code == 1
        assert "DRIFT DETECTED" in result.output
        assert "missing_column" in result.output

    def test_drift_cli_requires_baseline_or_dictionary(self):
        from click.testing import CliRunner
        from app.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "drift",
            "-c", "postgresql://u:p@localhost/db",
        ])
        assert result.exit_code == 2

    @patch("app.core.drift_detect.introspect_schema")
    def test_drift_cli_with_profile(self, mock_introspect, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        # Use RLS dict
        dict_file = tmp_path / "rls.json"
        dict_file.write_text(json.dumps(DICT_WITH_RLS), encoding="utf-8")

        dd = DataDictionary.model_validate(DICT_WITH_RLS)
        actual = dictionary_to_desired(dd)
        actual["schemas"] = {
            "platform": {"tables": {"tenant": actual["tables"]["platform.tenant"]}}
        }
        mock_introspect.return_value = actual

        runner = CliRunner()
        result = runner.invoke(cli, [
            "drift",
            "-c", "postgresql://u:p@localhost/db",
            "-d", str(dict_file),
            "-p", "standard",
            "-j",
        ])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert any(i["drift_type"] == "missing_rls" for i in output["items"])
