"""hb drift — Dictionary-based drift detection.

Compares a live database against the data dictionary (source of truth)
to find unauthorized changes: extra columns, missing columns, type
mismatches, missing indexes, missing RLS policies, missing masking views.

PBI #548 — Feature #541.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from app.core.compile import DataDictionary, load_dictionary, resolve_type
from app.core.introspect import introspect_schema
from app.core.plan import dictionary_to_desired
from app.models.schemas import DriftItem, DriftReport, DriftType


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def detect_drift(
    connection_string: str,
    dictionary_path: str,
    engine: str = "pg",
    profile: str = "essential",
    schema_filter: str | None = None,
) -> DriftReport:
    """Compare live DB against dictionary, return drift report."""
    dd = load_dictionary(dictionary_path)
    desired = dictionary_to_desired(dd, engine=engine, schema_filter=schema_filter)
    actual = introspect_schema(connection_string, schema=schema_filter)

    items: list[DriftItem] = []

    # Structural drift
    items.extend(_detect_table_drift(actual, desired))
    items.extend(_detect_column_drift(actual, desired))
    items.extend(_detect_index_drift(actual, desired))

    # Security drift (profile-dependent)
    if profile in ("standard", "enterprise"):
        items.extend(_detect_rls_drift(actual, dd, schema_filter))
    if profile == "enterprise":
        items.extend(_detect_masking_drift(actual, dd, schema_filter))

    summary = dict(Counter(item.drift_type.value for item in items))

    return DriftReport(
        has_drift=len(items) > 0,
        items=items,
        summary=summary,
        profile=profile,
        engine=engine,
        schema_filter=schema_filter,
    )


def detect_drift_from_schemas(
    actual: dict[str, Any],
    desired: dict[str, Any],
    dictionary: DataDictionary | None = None,
    profile: str = "essential",
    schema_filter: str | None = None,
) -> DriftReport:
    """Detect drift from pre-loaded schemas (for testing without DB)."""
    items: list[DriftItem] = []

    items.extend(_detect_table_drift(actual, desired))
    items.extend(_detect_column_drift(actual, desired))
    items.extend(_detect_index_drift(actual, desired))

    if dictionary and profile in ("standard", "enterprise"):
        items.extend(_detect_rls_drift(actual, dictionary, schema_filter))
    if dictionary and profile == "enterprise":
        items.extend(_detect_masking_drift(actual, dictionary, schema_filter))

    summary = dict(Counter(item.drift_type.value for item in items))

    return DriftReport(
        has_drift=len(items) > 0,
        items=items,
        summary=summary,
        profile=profile,
    )


# ---------------------------------------------------------------------------
# Table drift
# ---------------------------------------------------------------------------

def _detect_table_drift(
    actual: dict[str, Any],
    desired: dict[str, Any],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    actual_tables = set(actual.get("tables", {}).keys())
    desired_tables = set(desired.get("tables", {}).keys())

    for t in actual_tables - desired_tables:
        items.append(DriftItem(
            drift_type=DriftType.EXTRA_TABLE,
            object_name=t,
            details={"in_db": True, "in_dictionary": False},
            suggested_action=f"DROP TABLE {t}; or add entity to dictionary",
        ))

    for t in desired_tables - actual_tables:
        items.append(DriftItem(
            drift_type=DriftType.MISSING_TABLE,
            object_name=t,
            details={"in_db": False, "in_dictionary": True},
            suggested_action=f"Run 'hb apply' to create {t}",
        ))

    return items


# ---------------------------------------------------------------------------
# Column drift
# ---------------------------------------------------------------------------

def _detect_column_drift(
    actual: dict[str, Any],
    desired: dict[str, Any],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    actual_tables = actual.get("tables", {})
    desired_tables = desired.get("tables", {})

    for table_name in actual_tables:
        if table_name not in desired_tables:
            continue  # handled by table drift

        actual_cols = actual_tables[table_name].get("columns", {})
        desired_cols = desired_tables[table_name].get("columns", {})

        # Extra columns in DB
        for col in set(actual_cols) - set(desired_cols):
            items.append(DriftItem(
                drift_type=DriftType.EXTRA_COLUMN,
                object_name=f"{table_name}.{col}",
                details={"actual_type": actual_cols[col].get("type", "?")},
                suggested_action=f"ALTER TABLE {table_name} DROP COLUMN {col}; or add to dictionary",
            ))

        # Missing columns
        for col in set(desired_cols) - set(actual_cols):
            items.append(DriftItem(
                drift_type=DriftType.MISSING_COLUMN,
                object_name=f"{table_name}.{col}",
                details={"desired_type": desired_cols[col].get("type", "?")},
                suggested_action=f"Run 'hb apply' to add column {col} to {table_name}",
            ))

        # Type mismatches
        for col in set(actual_cols) & set(desired_cols):
            actual_type = actual_cols[col].get("type", "").upper()
            desired_type = desired_cols[col].get("type", "").upper()
            if actual_type != desired_type:
                items.append(DriftItem(
                    drift_type=DriftType.TYPE_MISMATCH,
                    object_name=f"{table_name}.{col}",
                    details={"actual": actual_type, "desired": desired_type},
                    suggested_action=f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE {desired_type};",
                ))

    return items


# ---------------------------------------------------------------------------
# Index drift
# ---------------------------------------------------------------------------

def _detect_index_drift(
    actual: dict[str, Any],
    desired: dict[str, Any],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    actual_tables = actual.get("tables", {})
    desired_tables = desired.get("tables", {})

    for table_name in desired_tables:
        if table_name not in actual_tables:
            continue

        actual_idx = set(actual_tables[table_name].get("indexes", {}).keys())
        desired_idx = set(desired_tables[table_name].get("indexes", {}).keys())

        for idx in desired_idx - actual_idx:
            idx_def = desired_tables[table_name]["indexes"][idx]
            cols = ", ".join(idx_def.get("columns", []))
            unique = "UNIQUE " if idx_def.get("unique") else ""
            items.append(DriftItem(
                drift_type=DriftType.MISSING_INDEX,
                object_name=f"{table_name}.{idx}",
                details={"columns": idx_def.get("columns", []), "unique": idx_def.get("unique", False)},
                suggested_action=f"CREATE {unique}INDEX {idx} ON {table_name} ({cols});",
            ))

        for idx in actual_idx - desired_idx:
            items.append(DriftItem(
                drift_type=DriftType.EXTRA_INDEX,
                object_name=f"{table_name}.{idx}",
                suggested_action=f"DROP INDEX {idx}; or add to dictionary",
            ))

    return items


# ---------------------------------------------------------------------------
# RLS drift (standard+)
# ---------------------------------------------------------------------------

def _detect_rls_drift(
    actual: dict[str, Any],
    dictionary: DataDictionary,
    schema_filter: str | None = None,
) -> list[DriftItem]:
    """Check that entities requiring RLS actually have it enabled in the DB.

    We check pg_policies via introspect data — if not available, we note
    which entities SHOULD have RLS based on dictionary config.
    """
    items: list[DriftItem] = []
    entities = dictionary.entities
    if schema_filter:
        entities = [e for e in entities if e.schema_name == schema_filter]

    for entity in entities:
        needs_rls = entity.security.get("rls", False)
        if not needs_rls and entity.multi_tenant is True:
            has_tenant = any(c.name == "tenant_id" for c in entity.columns)
            if has_tenant:
                needs_rls = True

        if not needs_rls:
            continue

        qualified = f"{entity.schema_name}.{entity.name}"
        # Check if table exists in actual schema
        actual_schemas = actual.get("schemas", {})
        schema_data = actual_schemas.get(entity.schema_name, {})
        table_data = schema_data.get("tables", {}).get(entity.name, {})

        if not table_data:
            continue  # table doesn't exist yet — not a drift issue

        # Check for RLS indicators in check_constraints or other introspect data
        # Since introspect doesn't directly report RLS status, we flag it as
        # "needs verification" — the suggested action tells the user what to check
        items.append(DriftItem(
            drift_type=DriftType.MISSING_RLS,
            object_name=qualified,
            details={"multi_tenant": entity.multi_tenant, "rls_required": True},
            suggested_action=(
                f"Verify RLS is enabled: "
                f"SELECT relrowsecurity FROM pg_class WHERE relname = '{entity.name}'; "
                f"If FALSE, run: ALTER TABLE {qualified} ENABLE ROW LEVEL SECURITY;"
            ),
        ))

    return items


# ---------------------------------------------------------------------------
# Masking drift (enterprise)
# ---------------------------------------------------------------------------

def _detect_masking_drift(
    actual: dict[str, Any],
    dictionary: DataDictionary,
    schema_filter: str | None = None,
) -> list[DriftItem]:
    """Check that entities with masking config have corresponding views."""
    items: list[DriftItem] = []
    entities = dictionary.entities
    if schema_filter:
        entities = [e for e in entities if e.schema_name == schema_filter]

    for entity in entities:
        masking_config = entity.security.get("masking", {})
        if not masking_config:
            continue

        view_name = f"{entity.schema_name}.v_{entity.name}_masked"

        # Check if the masking view exists in actual tables
        # (views appear as tables in introspect for some drivers)
        actual_tables = actual.get("tables", {})
        if view_name not in actual_tables:
            items.append(DriftItem(
                drift_type=DriftType.MISSING_MASKING,
                object_name=view_name,
                details={"masked_columns": list(masking_config.keys())},
                suggested_action=(
                    f"Run 'hb compile -p enterprise' to generate masking views, "
                    f"then apply 008_masking.sql"
                ),
            ))

    return items
