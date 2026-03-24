"""PostgreSQL schema introspection using SQLAlchemy."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, inspect, text


def introspect_schema(
    connection_string: str,
    schema: str | None = None,
) -> dict[str, Any]:
    """Return the actual schema of a PostgreSQL database as a normalized dict.

    If schema is provided, only that schema is introspected.
    If schema is None, all non-system schemas are introspected.
    """
    engine = create_engine(connection_string)
    inspector = inspect(engine)

    schemas_to_inspect = _resolve_schemas(inspector, schema)
    result: dict[str, Any] = {"schemas": {}}

    for s in schemas_to_inspect:
        tables = _introspect_tables(inspector, engine, s)
        if tables:
            result["schemas"][s] = {"tables": tables}

    # Backward-compatible flat "tables" view (default schema or single schema)
    result["tables"] = _flatten_tables(result["schemas"])

    engine.dispose()
    return result


def _resolve_schemas(inspector: Any, schema: str | None) -> list[str]:
    """Determine which schemas to introspect."""
    if schema:
        return [schema]

    system_schemas = {"information_schema", "pg_catalog", "pg_toast"}
    all_schemas = inspector.get_schema_names()
    return [s for s in all_schemas if s not in system_schemas]


def _introspect_tables(
    inspector: Any, engine: Any, schema: str
) -> dict[str, Any]:
    """Introspect all tables in a given schema."""
    tables: dict[str, Any] = {}

    for table_name in inspector.get_table_names(schema=schema):
        columns = _get_columns(inspector, table_name, schema)
        indexes = _get_indexes(inspector, table_name, schema)
        pk = inspector.get_pk_constraint(table_name, schema=schema)
        fks = _get_foreign_keys(inspector, table_name, schema)
        uniques = _get_unique_constraints(inspector, table_name, schema)
        checks = _get_check_constraints(inspector, table_name, schema)
        comments = _get_comments(engine, table_name, schema)

        tables[table_name] = {
            "columns": columns,
            "indexes": indexes,
            "primary_key": pk.get("constrained_columns", []) if pk else [],
            "foreign_keys": fks,
            "unique_constraints": uniques,
            "check_constraints": checks,
            "comment": comments.get("table"),
            "column_comments": comments.get("columns", {}),
        }

    return tables


def _get_columns(
    inspector: Any, table_name: str, schema: str
) -> dict[str, Any]:
    """Extract column definitions."""
    columns: dict[str, Any] = {}
    for col in inspector.get_columns(table_name, schema=schema):
        columns[col["name"]] = {
            "type": str(col["type"]),
            "nullable": col.get("nullable", True),
            "default": str(col["default"]) if col.get("default") else None,
        }
    return columns


def _get_indexes(
    inspector: Any, table_name: str, schema: str
) -> dict[str, Any]:
    """Extract index definitions."""
    indexes: dict[str, Any] = {}
    for idx in inspector.get_indexes(table_name, schema=schema):
        indexes[idx["name"]] = {
            "columns": idx["column_names"],
            "unique": idx.get("unique", False),
        }
    return indexes


def _get_foreign_keys(
    inspector: Any, table_name: str, schema: str
) -> list[dict[str, Any]]:
    """Extract foreign key relationships."""
    fks: list[dict[str, Any]] = []
    for fk in inspector.get_foreign_keys(table_name, schema=schema):
        fks.append({
            "name": fk.get("name"),
            "constrained_columns": fk["constrained_columns"],
            "referred_schema": fk.get("referred_schema", schema),
            "referred_table": fk["referred_table"],
            "referred_columns": fk["referred_columns"],
        })
    return fks


def _get_unique_constraints(
    inspector: Any, table_name: str, schema: str
) -> list[dict[str, Any]]:
    """Extract UNIQUE constraints."""
    uniques: list[dict[str, Any]] = []
    for uc in inspector.get_unique_constraints(table_name, schema=schema):
        uniques.append({
            "name": uc.get("name"),
            "columns": uc["column_names"],
        })
    return uniques


def _get_check_constraints(
    inspector: Any, table_name: str, schema: str
) -> list[dict[str, Any]]:
    """Extract CHECK constraints."""
    checks: list[dict[str, Any]] = []
    for cc in inspector.get_check_constraints(table_name, schema=schema):
        checks.append({
            "name": cc.get("name"),
            "expression": cc.get("sqltext", ""),
        })
    return checks


def _get_comments(
    engine: Any, table_name: str, schema: str
) -> dict[str, Any]:
    """Extract COMMENT ON for table and columns using pg_description."""
    result: dict[str, Any] = {"table": None, "columns": {}}

    try:
        with engine.connect() as conn:
            # Table comment
            row = conn.execute(text(
                "SELECT obj_description(c.oid) "
                "FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relname = :table AND n.nspname = :schema"
            ), {"table": table_name, "schema": schema}).fetchone()

            if row and row[0]:
                result["table"] = row[0]

            # Column comments
            rows = conn.execute(text(
                "SELECT a.attname, d.description "
                "FROM pg_attribute a "
                "JOIN pg_class c ON c.oid = a.attrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum "
                "WHERE c.relname = :table AND n.nspname = :schema "
                "AND a.attnum > 0 AND NOT a.attisdropped AND d.description IS NOT NULL"
            ), {"table": table_name, "schema": schema}).fetchall()

            for r in rows:
                result["columns"][r[0]] = r[1]

    except Exception:
        # Non-PostgreSQL or permission issues — comments are optional
        pass

    return result


def _flatten_tables(schemas: dict[str, Any]) -> dict[str, Any]:
    """Create a flat tables dict for backward compatibility.

    If single schema, returns its tables directly.
    If multiple schemas, prefixes table names with schema.
    """
    all_tables: dict[str, Any] = {}
    schema_names = list(schemas.keys())

    if len(schema_names) == 1:
        return schemas[schema_names[0]].get("tables", {})

    for s, data in schemas.items():
        for tname, tdef in data.get("tables", {}).items():
            all_tables[f"{s}.{tname}"] = tdef

    return all_tables
