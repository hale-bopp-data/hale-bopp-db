"""Tests for the schema diff engine."""

from hale_bopp_db.core.diff import compute_diff
from hale_bopp_db.models.schemas import ChangeType, RiskLevel


def test_add_new_table():
    actual = {"tables": {}}
    desired = {
        "tables": {
            "users": {
                "columns": {
                    "id": {"type": "INTEGER", "nullable": False},
                    "name": {"type": "VARCHAR(255)", "nullable": True},
                },
                "indexes": {},
                "primary_key": ["id"],
            }
        }
    }
    changes, risk = compute_diff(actual, desired)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.ADD_TABLE
    assert changes[0].object_name == "users"
    assert risk == RiskLevel.LOW


def test_drop_table_is_high_risk():
    actual = {
        "tables": {
            "legacy": {
                "columns": {"id": {"type": "INTEGER"}},
                "indexes": {},
                "primary_key": [],
            }
        }
    }
    desired = {"tables": {}}
    changes, risk = compute_diff(actual, desired)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.DROP_TABLE
    assert risk == RiskLevel.HIGH


def test_add_column():
    actual = {
        "tables": {
            "users": {
                "columns": {"id": {"type": "INTEGER"}},
                "indexes": {},
                "primary_key": [],
            }
        }
    }
    desired = {
        "tables": {
            "users": {
                "columns": {
                    "id": {"type": "INTEGER"},
                    "email": {"type": "VARCHAR(255)", "nullable": False},
                },
                "indexes": {},
                "primary_key": [],
            }
        }
    }
    changes, risk = compute_diff(actual, desired)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.ADD_COLUMN
    assert changes[0].object_name == "users.email"
    assert risk == RiskLevel.LOW


def test_alter_column_is_medium_risk():
    actual = {
        "tables": {
            "users": {
                "columns": {"name": {"type": "VARCHAR(100)"}},
                "indexes": {},
                "primary_key": [],
            }
        }
    }
    desired = {
        "tables": {
            "users": {
                "columns": {"name": {"type": "TEXT"}},
                "indexes": {},
                "primary_key": [],
            }
        }
    }
    changes, risk = compute_diff(actual, desired)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.ALTER_COLUMN
    assert risk == RiskLevel.MEDIUM


def test_no_changes():
    schema = {
        "tables": {
            "users": {
                "columns": {"id": {"type": "INTEGER"}},
                "indexes": {},
                "primary_key": [],
            }
        }
    }
    changes, risk = compute_diff(schema, schema)
    assert len(changes) == 0
    assert risk == RiskLevel.LOW
