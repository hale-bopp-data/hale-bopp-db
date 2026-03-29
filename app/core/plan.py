"""hb plan/apply — Dictionary-driven schema planning and deployment.

Reads the data dictionary (desired state), introspects a live DB (actual state),
computes the diff, and produces an auditable plan file.  Apply reads the plan
and executes it transactionally.

PBI #547 — Feature #541.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.compile import DataDictionary, load_dictionary, resolve_type
from app.core.deploy import deploy_changes
from app.core.diff import compute_diff
from app.core.introspect import introspect_schema
from app.models.schemas import PlanMetadata, PlanResult, SchemaChange


# ---------------------------------------------------------------------------
# Bridge: DataDictionary → introspect-compatible desired schema
# ---------------------------------------------------------------------------

def dictionary_to_desired(
    dictionary: DataDictionary,
    engine: str = "pg",
    schema_filter: str | None = None,
) -> dict[str, Any]:
    """Convert a DataDictionary into the dict format returned by introspect_schema.

    This is the bridge between the dictionary world (entities, logical types)
    and the diff world (tables, resolved PG types).
    """
    entities = dictionary.entities
    if schema_filter:
        entities = [e for e in entities if e.schema_name == schema_filter]

    type_map = dictionary.type_map

    # Build entity→schema lookup for FK resolution
    entity_schema_map: dict[str, str] = {}
    for e in dictionary.entities:
        entity_schema_map[e.name] = e.schema_name
        if e.id:
            entity_schema_map[e.id] = e.schema_name

    schemas: dict[str, Any] = {}
    flat_tables: dict[str, Any] = {}

    for entity in entities:
        table_def = _entity_to_table(entity, type_map, engine, entity_schema_map)
        schema_name = entity.schema_name

        if schema_name not in schemas:
            schemas[schema_name] = {"tables": {}}
        schemas[schema_name]["tables"][entity.name] = table_def

        # Flat key: schema.table (matches introspect _flatten_tables for multi-schema)
        flat_tables[f"{schema_name}.{entity.name}"] = table_def

    return {"schemas": schemas, "tables": flat_tables}


def _entity_to_table(
    entity: Any,
    type_map: dict[str, dict[str, str]],
    engine: str,
    entity_schema_map: dict[str, str],
) -> dict[str, Any]:
    """Convert a single EntityDef into introspect-format table dict."""
    columns: dict[str, Any] = {}
    pk_columns: list[str] = []
    foreign_keys: list[dict[str, Any]] = []
    indexes: dict[str, Any] = {}

    # Determine PK columns
    if entity.pk:
        pk_columns = entity.pk.get("columns", [])
    for col in entity.columns:
        if col.pk and col.name not in pk_columns:
            pk_columns.append(col.name)

    for col in entity.columns:
        pg_type = resolve_type(col.type, type_map, engine)
        columns[col.name] = {
            "type": pg_type,
            "nullable": col.nullable,
            "default": col.default,
        }

        # FK
        if col.fk:
            fk_info = _parse_fk(entity.schema_name, col, entity_schema_map)
            if fk_info:
                foreign_keys.append(fk_info)

    # Indexes
    for idx in entity.indexes:
        indexes[idx.name] = {
            "columns": idx.columns,
            "unique": idx.unique,
        }

    return {
        "columns": columns,
        "indexes": indexes,
        "primary_key": pk_columns,
        "foreign_keys": foreign_keys,
        "unique_constraints": [],
        "check_constraints": [],
        "comment": entity.description,
        "column_comments": {
            col.name: col.description
            for col in entity.columns
            if col.description
        },
    }


def _parse_fk(
    source_schema: str,
    col: Any,
    entity_schema_map: dict[str, str],
) -> dict[str, Any] | None:
    """Parse an FK reference string into introspect-format FK dict."""
    if not col.fk:
        return None

    parts = col.fk.split(".")
    if len(parts) == 2:
        ref_table, ref_col = parts
        ref_schema = entity_schema_map.get(ref_table, source_schema)
    elif len(parts) == 3:
        ref_schema, ref_table, ref_col = parts
    else:
        return None

    return {
        "name": f"fk_{col.name}",
        "constrained_columns": [col.name],
        "referred_schema": ref_schema,
        "referred_table": ref_table,
        "referred_columns": [ref_col],
    }


# ---------------------------------------------------------------------------
# Hashing utilities
# ---------------------------------------------------------------------------

def _file_hash(path: str | Path) -> str:
    """SHA-256 of a file's contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _changes_hash(changes: list[SchemaChange]) -> str:
    """SHA-256 of the serialized changes list (tamper detection)."""
    payload = json.dumps(
        [c.model_dump() for c in changes],
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sanitize_conn(conn: str) -> str:
    """Remove password from connection string for audit."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", conn)


# ---------------------------------------------------------------------------
# Plan: create
# ---------------------------------------------------------------------------

def create_plan(
    connection_string: str,
    dictionary_path: str | Path,
    engine: str = "pg",
    schema_filter: str | None = None,
) -> PlanResult:
    """Create a plan by comparing dictionary (desired) vs live DB (actual).

    Returns a PlanResult ready to be saved or reviewed.
    """
    dictionary_path = Path(dictionary_path)

    # Load & validate dictionary
    dd = load_dictionary(dictionary_path)

    # Convert dictionary → desired schema (introspect format)
    desired = dictionary_to_desired(dd, engine=engine, schema_filter=schema_filter)

    # Introspect live DB
    actual = introspect_schema(connection_string, schema=schema_filter)

    # Compute diff
    changes, risk = compute_diff(actual, desired)

    # Build rollback SQL
    rollback_parts = [c.sql_down for c in changes if c.sql_down]
    rollback_sql = "\n".join(reversed(rollback_parts))

    # Summary counts
    summary = dict(Counter(c.change_type.value for c in changes))

    # Metadata
    metadata = PlanMetadata(
        created_at=datetime.now(timezone.utc).isoformat(),
        dictionary_path=str(dictionary_path),
        dictionary_hash=_file_hash(dictionary_path),
        connection=_sanitize_conn(connection_string),
        engine=engine,
        schema_filter=schema_filter,
    )

    return PlanResult(
        metadata=metadata,
        changes=changes,
        risk_level=risk,
        plan_hash=_changes_hash(changes),
        rollback_sql=rollback_sql,
        summary=summary,
    )


def save_plan(plan: PlanResult, output_path: str | Path) -> Path:
    """Write a plan to a JSON file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(plan.model_dump_json(indent=2))
    return path


def load_plan(plan_path: str | Path) -> PlanResult:
    """Load a plan from a JSON file and verify its integrity."""
    with open(plan_path, encoding="utf-8") as f:
        raw = json.load(f)
    plan = PlanResult.model_validate(raw)

    # Verify plan hash (tamper detection)
    expected_hash = _changes_hash(plan.changes)
    if plan.plan_hash and plan.plan_hash != expected_hash:
        raise ValueError(
            f"Plan integrity check failed: expected hash {plan.plan_hash[:12]}..., "
            f"got {expected_hash[:12]}... — plan file may have been tampered with."
        )

    return plan


# ---------------------------------------------------------------------------
# Apply: execute a plan
# ---------------------------------------------------------------------------

def apply_plan(
    connection_string: str,
    plan: PlanResult,
    dry_run: bool = True,
) -> tuple[list[SchemaChange], str]:
    """Apply a plan's changes to a live database.

    Returns (applied_changes, rollback_sql).
    Delegates to the existing deploy engine for transactional execution.
    """
    return deploy_changes(
        connection_string=connection_string,
        changes=plan.changes,
        dry_run=dry_run,
    )
