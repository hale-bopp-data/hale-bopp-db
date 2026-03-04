"""Tests for DB-HALE-BOPP API endpoints (no database required)."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health():
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "version" in data


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
