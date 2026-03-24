"""Tests for the Maetel ER diagram generator."""

from app.core.maetel import to_json, to_mermaid


# --- Fixtures ---

SCHEMA_WITH_FK = {
    "schemas": {
        "platform": {
            "tables": {
                "tenant": {
                    "columns": {
                        "tenant_id": {"type": "TEXT", "nullable": False},
                        "tenant_name": {"type": "TEXT", "nullable": False},
                        "status": {"type": "TEXT", "nullable": False},
                        "created_at": {"type": "TIMESTAMPTZ", "nullable": False},
                    },
                    "indexes": {},
                    "primary_key": ["tenant_id"],
                    "foreign_keys": [],
                    "unique_constraints": [],
                    "check_constraints": [
                        {"name": "ck_tenant_status", "expression": "status IN ('ACTIVE','SUSPENDED','DELETED')"}
                    ],
                    "comment": "Root entity — every row belongs to a tenant",
                    "column_comments": {
                        "tenant_id": "Unique tenant identifier",
                    },
                },
                "user": {
                    "columns": {
                        "user_id": {"type": "TEXT", "nullable": False},
                        "tenant_id": {"type": "TEXT", "nullable": False},
                        "email": {"type": "TEXT", "nullable": False},
                        "display_name": {"type": "TEXT", "nullable": True},
                        "is_active": {"type": "BOOLEAN", "nullable": False},
                    },
                    "indexes": {"ix_user_tenant": {"columns": ["tenant_id"], "unique": False}},
                    "primary_key": ["user_id"],
                    "foreign_keys": [
                        {
                            "name": "fk_user_tenant",
                            "constrained_columns": ["tenant_id"],
                            "referred_schema": "platform",
                            "referred_table": "tenant",
                            "referred_columns": ["tenant_id"],
                        }
                    ],
                    "unique_constraints": [
                        {"name": "uq_user_tenant_email", "columns": ["tenant_id", "email"]}
                    ],
                    "check_constraints": [
                        {"name": "ck_user_email_format", "expression": "email ~* '^[^@]+@[^@]+\\.[^@]+$'"}
                    ],
                    "comment": "Portal users — scoped per tenant",
                    "column_comments": {},
                },
                "notification": {
                    "columns": {
                        "notification_id": {"type": "TEXT", "nullable": False},
                        "tenant_id": {"type": "TEXT", "nullable": False},
                        "user_id": {"type": "TEXT", "nullable": False},
                        "message": {"type": "TEXT", "nullable": False},
                    },
                    "indexes": {},
                    "primary_key": ["notification_id"],
                    "foreign_keys": [
                        {
                            "name": "fk_notif_tenant",
                            "constrained_columns": ["tenant_id"],
                            "referred_schema": "platform",
                            "referred_table": "tenant",
                            "referred_columns": ["tenant_id"],
                        },
                        {
                            "name": "fk_notif_user",
                            "constrained_columns": ["user_id"],
                            "referred_schema": "platform",
                            "referred_table": "user",
                            "referred_columns": ["user_id"],
                        },
                    ],
                    "unique_constraints": [],
                    "check_constraints": [],
                    "comment": None,
                    "column_comments": {},
                },
            }
        }
    },
    "tables": {
        "tenant": {},  # flat view (simplified for tests)
        "user": {},
        "notification": {},
    },
}

EMPTY_SCHEMA = {"schemas": {}, "tables": {}}


# --- Mermaid tests ---


def test_mermaid_starts_with_erdiagram():
    result = to_mermaid(SCHEMA_WITH_FK, schema_name="platform")
    assert result.startswith("erDiagram")


def test_mermaid_contains_relationships():
    result = to_mermaid(SCHEMA_WITH_FK, schema_name="platform")
    # tenant -> user relationship should exist
    assert "tenant" in result
    assert "user" in result
    # Should have relationship lines with ||--o{ or similar
    assert "||--o{" in result or "||--||" in result


def test_mermaid_contains_pk_markers():
    result = to_mermaid(SCHEMA_WITH_FK, schema_name="platform")
    assert "PK" in result


def test_mermaid_contains_fk_markers():
    result = to_mermaid(SCHEMA_WITH_FK, schema_name="platform")
    assert "FK" in result


def test_mermaid_sanitizes_types():
    """Types with parentheses should be cleaned for Mermaid."""
    schema = {
        "schemas": {
            "test": {
                "tables": {
                    "t1": {
                        "columns": {
                            "amount": {"type": "NUMERIC(18,2)", "nullable": False},
                        },
                        "indexes": {},
                        "primary_key": [],
                        "foreign_keys": [],
                        "unique_constraints": [],
                        "check_constraints": [],
                        "comment": None,
                        "column_comments": {},
                    }
                }
            }
        },
        "tables": {},
    }
    result = to_mermaid(schema, schema_name="test")
    # Should not contain raw parentheses
    assert "NUMERIC(18,2)" not in result
    assert "numeric18_2" in result


def test_mermaid_empty_schema():
    result = to_mermaid(EMPTY_SCHEMA)
    assert "No tables found" in result


def test_mermaid_includes_column_comments():
    result = to_mermaid(SCHEMA_WITH_FK, schema_name="platform")
    assert "Unique tenant identifier" in result


def test_mermaid_relationship_labels():
    result = to_mermaid(SCHEMA_WITH_FK, schema_name="platform")
    # FK label should be derived from column name (tenant_id -> "tenant")
    assert '"tenant"' in result


# --- JSON tests ---


def test_json_structure():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    assert "entities" in result
    assert "relationships" in result
    assert "stats" in result


def test_json_entity_count():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    assert result["stats"]["entity_count"] == 3


def test_json_relationship_count():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    # user->tenant, notification->tenant, notification->user = 3
    assert result["stats"]["relationship_count"] == 3


def test_json_entity_has_attributes():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    tenant = next(e for e in result["entities"] if e["name"] == "tenant")
    assert len(tenant["attributes"]) == 4
    assert tenant["comment"] == "Root entity — every row belongs to a tenant"


def test_json_entity_pk_marked():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    tenant = next(e for e in result["entities"] if e["name"] == "tenant")
    pk_attr = next(a for a in tenant["attributes"] if a["name"] == "tenant_id")
    assert pk_attr["is_pk"] is True


def test_json_entity_fk_marked():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    user = next(e for e in result["entities"] if e["name"] == "user")
    fk_attr = next(a for a in user["attributes"] if a["name"] == "tenant_id")
    assert fk_attr["is_fk"] is True


def test_json_relationship_cardinality():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    user_tenant = next(
        r for r in result["relationships"]
        if r["from_entity"] == "user" and r["to_entity"] == "tenant"
    )
    assert user_tenant["cardinality"] == "one-to-many"


def test_json_includes_check_constraints():
    result = to_json(SCHEMA_WITH_FK, schema_name="platform")
    tenant = next(e for e in result["entities"] if e["name"] == "tenant")
    assert len(tenant["check_constraints"]) == 1
    assert "ck_tenant_status" in tenant["check_constraints"][0]["name"]


def test_json_empty_schema():
    result = to_json(EMPTY_SCHEMA)
    assert result["stats"]["entity_count"] == 0
    assert result["stats"]["relationship_count"] == 0
