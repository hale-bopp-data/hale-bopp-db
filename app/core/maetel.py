"""Maetel — ER diagram generator from introspected schema.

Named after Maetel from Galaxy Express 999 (銀河鉄道999) by Leiji Matsumoto.
The mysterious guide who knows every stop on the endless rail across the cosmos —
just as this module knows every entity and relationship in your database schema.

Produces Mermaid erDiagram and structured JSON output
from the enriched introspection data (FK, constraints, comments).
"""

from __future__ import annotations

from typing import Any


def to_mermaid(schema: dict[str, Any], schema_name: str | None = None) -> str:
    """Generate a Mermaid erDiagram from introspected schema.

    Args:
        schema: Output of introspect_schema (with 'schemas' and 'tables' keys).
        schema_name: If provided, only render tables from this schema.

    Returns:
        Mermaid erDiagram string ready for rendering.
    """
    tables = _resolve_tables(schema, schema_name)
    if not tables:
        return "erDiagram\n    %% No tables found"

    lines: list[str] = ["erDiagram"]

    # Collect all relationships from FK definitions
    relationships = _extract_relationships(tables)
    for rel in relationships:
        lines.append(f"    {rel}")

    # Render each table with its attributes
    for table_name in sorted(tables.keys()):
        table_def = tables[table_name]
        lines.append("")
        lines.extend(_render_table(table_name, table_def))

    return "\n".join(lines)


def to_json(schema: dict[str, Any], schema_name: str | None = None) -> dict[str, Any]:
    """Generate a structured JSON representation (USD-like) of the schema.

    Args:
        schema: Output of introspect_schema.
        schema_name: If provided, only include tables from this schema.

    Returns:
        Structured dict with entities, attributes, relationships, constraints.
    """
    tables = _resolve_tables(schema, schema_name)
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []

    for table_name, table_def in sorted(tables.items()):
        entity = _build_entity(table_name, table_def)
        entities.append(entity)

        for fk in table_def.get("foreign_keys", []):
            ref_table = fk["referred_table"]
            ref_schema = fk.get("referred_schema")
            if ref_schema and schema_name and ref_schema != schema_name:
                ref_table = f"{ref_schema}.{ref_table}"

            relationships.append({
                "name": fk.get("name"),
                "from_entity": table_name,
                "from_columns": fk["constrained_columns"],
                "to_entity": ref_table,
                "to_columns": fk["referred_columns"],
                "cardinality": _infer_cardinality(table_name, fk, table_def),
            })

    return {
        "schema": schema_name or "all",
        "entities": entities,
        "relationships": relationships,
        "stats": {
            "entity_count": len(entities),
            "relationship_count": len(relationships),
        },
    }


# --- Internal helpers ---


def _resolve_tables(schema: dict[str, Any], schema_name: str | None) -> dict[str, Any]:
    """Get the right tables dict based on schema filter."""
    if schema_name and "schemas" in schema:
        schema_data = schema["schemas"].get(schema_name, {})
        return schema_data.get("tables", {})

    # Use flat tables view
    return schema.get("tables", {})


def _extract_relationships(tables: dict[str, Any]) -> list[str]:
    """Build Mermaid relationship lines from FK definitions."""
    lines: list[str] = []
    seen: set[tuple[str, str]] = set()

    for table_name, table_def in tables.items():
        for fk in table_def.get("foreign_keys", []):
            ref_table = fk["referred_table"]
            pair = (ref_table, table_name)
            if pair in seen:
                continue
            seen.add(pair)

            cardinality = _infer_cardinality(table_name, fk, table_def)
            mermaid_card = _cardinality_to_mermaid(cardinality)
            label = _fk_label(fk)
            lines.append(f'{ref_table} {mermaid_card} {table_name} : "{label}"')

    return lines


def _infer_cardinality(
    table_name: str, fk: dict[str, Any], table_def: dict[str, Any]
) -> str:
    """Infer relationship cardinality from constraints.

    Returns one of: "one-to-one", "one-to-many", "many-to-many".
    """
    fk_cols = fk["constrained_columns"]
    pk_cols = table_def.get("primary_key", [])

    # If FK columns ARE the PK → one-to-one
    if set(fk_cols) == set(pk_cols):
        return "one-to-one"

    # If FK columns are part of a composite PK → likely many-to-many bridge
    if set(fk_cols).issubset(set(pk_cols)) and len(pk_cols) > 1:
        return "many-to-many"

    # Check if FK columns have a UNIQUE constraint → one-to-one
    for uc in table_def.get("unique_constraints", []):
        if set(fk_cols) == set(uc.get("columns", [])):
            return "one-to-one"

    return "one-to-many"


def _cardinality_to_mermaid(cardinality: str) -> str:
    """Convert cardinality string to Mermaid notation."""
    mapping = {
        "one-to-one": "||--||",
        "one-to-many": "||--o{",
        "many-to-many": "}o--o{",
    }
    return mapping.get(cardinality, "||--o{")


def _fk_label(fk: dict[str, Any]) -> str:
    """Generate a readable label for a FK relationship."""
    cols = fk["constrained_columns"]
    if len(cols) == 1:
        col = cols[0]
        # Strip _id suffix for cleaner label
        label = col.removesuffix("_id")
        return label
    return ", ".join(cols)


def _render_table(table_name: str, table_def: dict[str, Any]) -> list[str]:
    """Render a single table as Mermaid entity block."""
    lines: list[str] = [f"    {table_name} {{"]
    columns = table_def.get("columns", {})
    pk_cols = set(table_def.get("primary_key", []))
    fk_cols = _collect_fk_columns(table_def)
    unique_cols = _collect_unique_columns(table_def)

    for col_name, col_info in columns.items():
        col_type = _sanitize_type(col_info.get("type", "TEXT"))
        markers = _column_markers(col_name, pk_cols, fk_cols, unique_cols)
        comment = _column_comment(col_name, table_def)

        if comment:
            lines.append(f'        {col_type} {col_name} {markers} "{comment}"')
        elif markers:
            lines.append(f"        {col_type} {col_name} {markers}")
        else:
            lines.append(f"        {col_type} {col_name}")

    lines.append("    }")
    return lines


def _collect_fk_columns(table_def: dict[str, Any]) -> set[str]:
    """Collect all columns that participate in foreign keys."""
    fk_cols: set[str] = set()
    for fk in table_def.get("foreign_keys", []):
        fk_cols.update(fk["constrained_columns"])
    return fk_cols


def _collect_unique_columns(table_def: dict[str, Any]) -> set[str]:
    """Collect all columns that have UNIQUE constraints."""
    unique_cols: set[str] = set()
    for uc in table_def.get("unique_constraints", []):
        if len(uc.get("columns", [])) == 1:
            unique_cols.update(uc["columns"])
    return unique_cols


def _column_markers(
    col_name: str,
    pk_cols: set[str],
    fk_cols: set[str],
    unique_cols: set[str],
) -> str:
    """Generate PK/FK/UK markers for a column."""
    markers: list[str] = []
    if col_name in pk_cols:
        markers.append("PK")
    if col_name in fk_cols:
        markers.append("FK")
    if col_name in unique_cols and col_name not in pk_cols:
        markers.append("UK")
    return ", ".join(markers)


def _column_comment(col_name: str, table_def: dict[str, Any]) -> str | None:
    """Get comment for a column if available."""
    comments = table_def.get("column_comments", {})
    return comments.get(col_name)


def _sanitize_type(type_str: str) -> str:
    """Clean up type string for Mermaid compatibility.

    Mermaid doesn't like parentheses in type names.
    VARCHAR(255) → varchar255, NUMERIC(18,2) → numeric18_2
    """
    result = type_str.lower()
    result = result.replace("(", "").replace(")", "").replace(",", "_")
    # Remove spaces
    result = result.replace(" ", "_")
    return result


def _build_entity(table_name: str, table_def: dict[str, Any]) -> dict[str, Any]:
    """Build a structured entity dict for JSON output."""
    pk_cols = set(table_def.get("primary_key", []))
    fk_cols = _collect_fk_columns(table_def)

    attributes: list[dict[str, Any]] = []
    for col_name, col_info in table_def.get("columns", {}).items():
        attr: dict[str, Any] = {
            "name": col_name,
            "type": col_info.get("type", "TEXT"),
            "nullable": col_info.get("nullable", True),
            "default": col_info.get("default"),
            "is_pk": col_name in pk_cols,
            "is_fk": col_name in fk_cols,
        }
        comment = table_def.get("column_comments", {}).get(col_name)
        if comment:
            attr["comment"] = comment
        attributes.append(attr)

    entity: dict[str, Any] = {
        "name": table_name,
        "attributes": attributes,
        "primary_key": table_def.get("primary_key", []),
        "unique_constraints": table_def.get("unique_constraints", []),
        "check_constraints": table_def.get("check_constraints", []),
    }

    table_comment = table_def.get("comment")
    if table_comment:
        entity["comment"] = table_comment

    return entity
