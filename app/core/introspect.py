"""PostgreSQL schema introspection using SQLAlchemy."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, inspect


def introspect_schema(connection_string: str) -> dict[str, Any]:
    """Return the actual schema of a PostgreSQL database as a normalized dict."""
    engine = create_engine(connection_string)
    inspector = inspect(engine)

    schema: dict[str, Any] = {"tables": {}}

    for table_name in inspector.get_table_names():
        columns = {}
        for col in inspector.get_columns(table_name):
            columns[col["name"]] = {
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "default": str(col["default"]) if col.get("default") else None,
            }

        indexes = {}
        for idx in inspector.get_indexes(table_name):
            indexes[idx["name"]] = {
                "columns": idx["column_names"],
                "unique": idx.get("unique", False),
            }

        pk = inspector.get_pk_constraint(table_name)

        schema["tables"][table_name] = {
            "columns": columns,
            "indexes": indexes,
            "primary_key": pk.get("constrained_columns", []) if pk else [],
        }

    engine.dispose()
    return schema
