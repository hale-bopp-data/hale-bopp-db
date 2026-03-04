"""Schema diff engine — compares actual (introspected) vs desired schema."""

from __future__ import annotations

from typing import Any

from app.models.schemas import ChangeType, RiskLevel, SchemaChange


def compute_diff(actual: dict[str, Any], desired: dict[str, Any]) -> tuple[list[SchemaChange], RiskLevel]:
    """Compare actual schema with desired schema, return changes and risk level."""
    changes: list[SchemaChange] = []
    actual_tables = actual.get("tables", {})
    desired_tables = desired.get("tables", {})

    # Tables to add
    for table_name in desired_tables:
        if table_name not in actual_tables:
            changes.append(SchemaChange(
                change_type=ChangeType.ADD_TABLE,
                object_name=table_name,
                details=desired_tables[table_name],
                sql_up=_gen_create_table(table_name, desired_tables[table_name]),
                sql_down=f'DROP TABLE IF EXISTS "{table_name}";',
            ))

    # Tables to drop
    for table_name in actual_tables:
        if table_name not in desired_tables:
            changes.append(SchemaChange(
                change_type=ChangeType.DROP_TABLE,
                object_name=table_name,
                sql_up=f'DROP TABLE IF EXISTS "{table_name}";',
                sql_down=_gen_create_table(table_name, actual_tables[table_name]),
            ))

    # Tables in both — compare columns
    for table_name in desired_tables:
        if table_name not in actual_tables:
            continue
        actual_cols = actual_tables[table_name].get("columns", {})
        desired_cols = desired_tables[table_name].get("columns", {})

        for col_name, col_def in desired_cols.items():
            if col_name not in actual_cols:
                col_type = col_def.get("type", "TEXT")
                nullable = "NULL" if col_def.get("nullable", True) else "NOT NULL"
                changes.append(SchemaChange(
                    change_type=ChangeType.ADD_COLUMN,
                    object_name=f"{table_name}.{col_name}",
                    details=col_def,
                    sql_up=f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type} {nullable};',
                    sql_down=f'ALTER TABLE "{table_name}" DROP COLUMN IF EXISTS "{col_name}";',
                ))
            else:
                actual_type = actual_cols[col_name].get("type", "")
                desired_type = col_def.get("type", "")
                if actual_type.upper() != desired_type.upper():
                    changes.append(SchemaChange(
                        change_type=ChangeType.ALTER_COLUMN,
                        object_name=f"{table_name}.{col_name}",
                        details={"from": actual_type, "to": desired_type},
                        sql_up=f'ALTER TABLE "{table_name}" ALTER COLUMN "{col_name}" TYPE {desired_type};',
                        sql_down=f'ALTER TABLE "{table_name}" ALTER COLUMN "{col_name}" TYPE {actual_type};',
                    ))

        for col_name in actual_cols:
            if col_name not in desired_cols:
                col_type = actual_cols[col_name].get("type", "TEXT")
                changes.append(SchemaChange(
                    change_type=ChangeType.DROP_COLUMN,
                    object_name=f"{table_name}.{col_name}",
                    sql_up=f'ALTER TABLE "{table_name}" DROP COLUMN IF EXISTS "{col_name}";',
                    sql_down=f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type};',
                ))

    risk = _assess_risk(changes)
    return changes, risk


def _assess_risk(changes: list[SchemaChange]) -> RiskLevel:
    destructive = {ChangeType.DROP_TABLE, ChangeType.DROP_COLUMN, ChangeType.ALTER_COLUMN}
    has_destructive = any(c.change_type in destructive for c in changes)
    has_drop_table = any(c.change_type == ChangeType.DROP_TABLE for c in changes)

    if has_drop_table:
        return RiskLevel.HIGH
    if has_destructive:
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def _gen_create_table(table_name: str, table_def: dict[str, Any]) -> str:
    columns = table_def.get("columns", {})
    col_defs = []
    for col_name, col_info in columns.items():
        col_type = col_info.get("type", "TEXT")
        nullable = "" if col_info.get("nullable", True) else " NOT NULL"
        col_defs.append(f'  "{col_name}" {col_type}{nullable}')

    pk_cols = table_def.get("primary_key", [])
    if pk_cols:
        pk_str = ", ".join(f'"{c}"' for c in pk_cols)
        col_defs.append(f"  PRIMARY KEY ({pk_str})")

    cols_sql = ",\n".join(col_defs)
    return f'CREATE TABLE "{table_name}" (\n{cols_sql}\n);'
