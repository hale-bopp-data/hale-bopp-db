"""Tests for DB-HALE-BOPP API endpoints (no database required)."""

from __future__ import annotations

import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Minimal dictionary for API tests
# ---------------------------------------------------------------------------

MINIMAL_DICT = {
    "type_map": {
        "auto": {"pg": "BIGSERIAL", "mssql": "BIGINT IDENTITY(1,1)"},
        "string(n)": {"pg": "VARCHAR({n})", "mssql": "NVARCHAR({n})"},
        "timestamp": {"pg": "TIMESTAMPTZ", "mssql": "DATETIME2"},
        "boolean": {"pg": "BOOLEAN", "mssql": "BIT"},
        "integer": {"pg": "INTEGER", "mssql": "INT"},
    },
    "default_map": {
        "now()": {"pg": "NOW()", "mssql": "SYSUTCDATETIME()"},
        "true": {"pg": "TRUE", "mssql": "1"},
    },
    "schemas": [{"name": "platform"}],
    "entities": [
        {
            "name": "tenant",
            "schema": "platform",
            "type": "DIM",
            "description": "Root entity.",
            "pk": {"columns": ["id"]},
            "columns": [
                {"name": "id", "type": "auto", "nullable": False},
                {"name": "name", "type": "string(100)", "nullable": False},
                {"name": "created_at", "type": "timestamp", "nullable": False, "default": "now()"},
            ],
            "indexes": [
                {"name": "ux_tenant_name", "columns": ["name"], "unique": True},
            ],
        },
    ],
}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health():
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "version" in data


# ---------------------------------------------------------------------------
# Diff (legacy)
# ---------------------------------------------------------------------------

@patch("app.api.routes.introspect_schema")
def test_schema_diff_add_table(mock_introspect):
    mock_introspect.return_value = {"tables": {}}

    resp = client.post("/api/v1/schema/diff", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "desired_schema": {
            "tables": {
                "orders": {
                    "columns": {
                        "id": {"type": "INTEGER", "nullable": False},
                        "total": {"type": "NUMERIC(10,2)", "nullable": True},
                    },
                    "indexes": {},
                    "primary_key": ["id"],
                }
            }
        },
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["changes"]) == 1
    assert data["changes"][0]["change_type"] == "add_table"
    assert data["risk_level"] == "low"


# ---------------------------------------------------------------------------
# Deploy (legacy)
# ---------------------------------------------------------------------------

@patch("app.api.routes.deploy_changes")
def test_schema_deploy_dry_run(mock_deploy):
    mock_deploy.return_value = ([], "-- rollback")

    resp = client.post("/api/v1/schema/deploy", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "changes": [],
        "dry_run": True,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["rollback_sql"] == "-- rollback"


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------

@patch("app.api.routes.introspect_schema")
def test_plan_empty_db(mock_introspect):
    mock_introspect.return_value = {"schemas": {}, "tables": {}}

    resp = client.post("/api/v1/plan", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "dictionary": MINIMAL_DICT,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["changes"]) > 0
    assert data["risk_level"] == "low"
    assert data["plan_hash"]
    assert "add_table" in data["summary"]


@patch("app.api.routes.introspect_schema")
def test_plan_no_changes(mock_introspect):
    """If DB matches dictionary, plan has no changes."""
    from app.core.compile import DataDictionary
    from app.core.plan import dictionary_to_desired
    dd = DataDictionary.model_validate(MINIMAL_DICT)
    mock_introspect.return_value = dictionary_to_desired(dd)

    resp = client.post("/api/v1/plan", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "dictionary": MINIMAL_DICT,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["changes"]) == 0


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

@patch("app.api.routes.apply_plan")
def test_apply_dry_run(mock_apply):
    mock_apply.return_value = ([], "-- rollback")

    plan = {
        "version": "1",
        "metadata": {
            "created_at": "2026-03-25T10:00:00+00:00",
            "dictionary_path": "<api>",
            "dictionary_hash": "",
            "connection": "postgresql://***@localhost/db",
            "engine": "pg",
        },
        "changes": [],
        "risk_level": "low",
        "plan_hash": "",
        "rollback_sql": "",
        "summary": {},
    }

    resp = client.post("/api/v1/apply", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "plan": plan,
        "dry_run": True,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["dry_run"] is True


# ---------------------------------------------------------------------------
# Drift (dictionary mode)
# ---------------------------------------------------------------------------

@patch("app.api.routes.introspect_schema")
def test_drift_dictionary_no_drift(mock_introspect):
    from app.core.compile import DataDictionary
    from app.core.plan import dictionary_to_desired
    dd = DataDictionary.model_validate(MINIMAL_DICT)
    mock_introspect.return_value = dictionary_to_desired(dd)

    resp = client.post("/api/v1/drift/dictionary", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "dictionary": MINIMAL_DICT,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["has_drift"] is False


@patch("app.api.routes.introspect_schema")
def test_drift_dictionary_detects_extra_column(mock_introspect):
    from app.core.compile import DataDictionary
    from app.core.plan import dictionary_to_desired
    dd = DataDictionary.model_validate(MINIMAL_DICT)
    actual = dictionary_to_desired(dd)
    actual["tables"]["platform.tenant"]["columns"]["rogue"] = {
        "type": "TEXT", "nullable": True, "default": None,
    }
    mock_introspect.return_value = actual

    resp = client.post("/api/v1/drift/dictionary", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "dictionary": MINIMAL_DICT,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["has_drift"] is True
    assert any(i["drift_type"] == "extra_column" for i in data["items"])


# ---------------------------------------------------------------------------
# Compile
# ---------------------------------------------------------------------------

def test_compile_pg():
    resp = client.post("/api/v1/compile", json={
        "dictionary": MINIMAL_DICT,
        "engine": "pg",
        "profile": "essential",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["engine"] == "pg"
    assert data["entity_count"] == 1
    assert len(data["files"]) >= 2
    file_names = [f["name"] for f in data["files"]]
    assert "002_tables.sql" in file_names


def test_compile_mssql():
    resp = client.post("/api/v1/compile", json={
        "dictionary": MINIMAL_DICT,
        "engine": "mssql",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["engine"] == "mssql"
    tables_file = next(f for f in data["files"] if f["name"] == "002_tables.sql")
    assert "IDENTITY(1,1)" in tables_file["content"]


def test_compile_redis_no_patterns():
    resp = client.post("/api/v1/compile", json={
        "dictionary": MINIMAL_DICT,
        "engine": "redis",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["engine"] == "redis"
    assert data["entity_count"] == 0


def test_compile_redis_with_patterns():
    dd_with_redis = {
        **MINIMAL_DICT,
        "redis_patterns": [
            {"use_case": "cache", "structure": "HASH", "key": "t:{id}:c", "ttl": 60, "strategy": "write-through"},
        ],
    }
    resp = client.post("/api/v1/compile", json={
        "dictionary": dd_with_redis,
        "engine": "redis",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["entity_count"] == 1
    file_names = [f["name"] for f in data["files"]]
    assert "redis-config.json" in file_names


# ---------------------------------------------------------------------------
# Agentic Observer / Testudo guardrails
# ---------------------------------------------------------------------------

def test_apply_suggestion_add_column():
    resp = client.post("/api/v1/agent/apply-suggestion", json={
        "dictionary": MINIMAL_DICT,
        "entity": "tenant",
        "change": "add_column",
        "field": {
            "name": "priority",
            "type": "integer",
            "nullable": True,
        },
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["applied"] is True
    assert data["risk"] == "low"
    columns = data["updated_dictionary"]["entities"][0]["columns"]
    assert any(col["name"] == "priority" for col in columns)


def test_apply_suggestion_drop_column_has_high_risk():
    resp = client.post("/api/v1/agent/apply-suggestion", json={
        "dictionary": MINIMAL_DICT,
        "entity": "tenant",
        "change": "drop_column",
        "field": {
            "name": "name",
        },
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["risk"] == "high"
    assert data["warnings"]
    columns = data["updated_dictionary"]["entities"][0]["columns"]
    assert all(col["name"] != "name" for col in columns)


def test_apply_suggestion_rejects_unknown_entity():
    resp = client.post("/api/v1/agent/apply-suggestion", json={
        "dictionary": MINIMAL_DICT,
        "entity": "orders",
        "change": "add_column",
        "field": {
            "name": "priority",
            "type": "integer",
        },
    })
    assert resp.status_code == 404


@patch("app.api.routes.introspect_schema")
def test_pr_comment_payload_no_changes(mock_introspect):
    from app.core.compile import DataDictionary
    from app.core.plan import dictionary_to_desired

    dd = DataDictionary.model_validate(MINIMAL_DICT)
    mock_introspect.return_value = dictionary_to_desired(dd)

    resp = client.post("/api/v1/git/pr-comment-payload", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "dictionary": MINIMAL_DICT,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "No schema changes detected" in data["markdown"]
    assert data["risk_level"] == "low"


@patch("app.api.routes.introspect_schema")
def test_pr_comment_payload_warns_on_drop(mock_introspect):
    mock_introspect.return_value = {
        "tables": {
            "platform.tenant": {
                "columns": {
                    "id": {"type": "BIGSERIAL", "nullable": False},
                    "name": {"type": "VARCHAR(100)", "nullable": False},
                    "created_at": {"type": "TIMESTAMPTZ", "nullable": False},
                    "rogue": {"type": "TEXT", "nullable": True},
                },
                "indexes": {
                    "ux_tenant_name": {"columns": ["name"], "unique": True},
                },
                "primary_key": ["id"],
            }
        },
        "schemas": {},
    }

    resp = client.post("/api/v1/git/pr-comment-payload", json={
        "connection_string": "postgresql://fake:fake@localhost/fake",
        "dictionary": MINIMAL_DICT,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "Destructive changes" in data["markdown"]
    assert "drop_column" in data["markdown"]
    assert data["warnings"]


def test_seed_generate_basic():
    resp = client.post("/api/v1/seed/generate", json={
        "dictionary": MINIMAL_DICT,
        "rows_per_table": 3,
        "locale": "it_IT",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "platform.tenant" in data["seed_data"]
    assert len(data["seed_data"]["platform.tenant"]) == 3
    assert data["stats"]["table_count"] == 1


def test_seed_generate_respects_fk_order():
    dictionary = {
        **MINIMAL_DICT,
        "entities": [
            {
                "name": "tenant",
                "schema": "platform",
                "type": "DIM",
                "pk": {"columns": ["id"]},
                "columns": [
                    {"name": "id", "type": "auto", "nullable": False},
                    {"name": "tenant_id", "type": "string(50)", "nullable": False},
                ],
            },
            {
                "name": "user",
                "schema": "platform",
                "type": "DIM",
                "pk": {"columns": ["id"]},
                "columns": [
                    {"name": "id", "type": "auto", "nullable": False},
                    {"name": "tenant_id", "type": "string(50)", "nullable": False, "fk": "tenant.tenant_id"},
                    {"name": "email", "type": "string(100)", "nullable": False},
                ],
            },
        ],
    }

    resp = client.post("/api/v1/seed/generate", json={
        "dictionary": dictionary,
        "rows_per_table": 2,
    })
    assert resp.status_code == 200
    data = resp.json()
    tenant_ids = [row["tenant_id"] for row in data["seed_data"]["platform.tenant"]]
    user_rows = data["seed_data"]["platform.user"]
    assert all(row["tenant_id"] in tenant_ids for row in user_rows)


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

def test_validate_passes():
    resp = client.post("/api/v1/validate", json={
        "dictionary": MINIMAL_DICT,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_checks"] > 0
    assert data["passed"] > 0


def test_validate_with_suite():
    resp = client.post("/api/v1/validate", json={
        "dictionary": MINIMAL_DICT,
        "suite": "naming",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_checks"] > 0
