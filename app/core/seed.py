"""Mock data seeder for Hale-Bopp DB (PBI-7)."""

from __future__ import annotations

import random
import re
from collections import defaultdict, deque
from typing import Any

from app.core.compile import DataDictionary, EntityDef


def generate_seed_data(
    dictionary: DataDictionary,
    rows_per_table: int = 10,
    locale: str = "it_IT",
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """
    Generate deterministic seed data that respects FK ordering and basic constraints.

    The implementation is intentionally deterministic-first: no DB access, no SQL execution,
    and no dependency on external providers.
    """
    entities = dictionary.entities
    ordered = _topological_entities(entities)
    generated: dict[str, list[dict[str, Any]]] = {}
    rng = random.Random(42)

    for entity in ordered:
        table_key = f"{entity.schema_name}.{entity.name}"
        rows: list[dict[str, Any]] = []
        for row_index in range(rows_per_table):
            row = _generate_entity_row(entity, row_index, generated, dictionary, rng, locale)
            rows.append(row)
        generated[table_key] = rows

    stats = {
        "table_count": len(generated),
        "row_count": sum(len(rows) for rows in generated.values()),
    }
    return generated, stats


def _topological_entities(entities: list[EntityDef]) -> list[EntityDef]:
    by_name = {entity.name: entity for entity in entities}
    in_degree = {entity.name: 0 for entity in entities}
    graph: dict[str, set[str]] = defaultdict(set)

    for entity in entities:
        for col in entity.columns:
            ref = _extract_fk_target_table(col.fk)
            if ref and ref in by_name and ref != entity.name and entity.name not in graph[ref]:
                graph[ref].add(entity.name)
                in_degree[entity.name] += 1

    queue = deque(sorted(name for name, degree in in_degree.items() if degree == 0))
    ordered: list[EntityDef] = []

    while queue:
        current = queue.popleft()
        ordered.append(by_name[current])
        for child in sorted(graph[current]):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(ordered) != len(entities):
        remaining = [entity for entity in entities if entity.name not in {item.name for item in ordered}]
        ordered.extend(sorted(remaining, key=lambda entity: entity.name))
    return ordered


def _extract_fk_target_table(fk: str | None) -> str | None:
    if not fk:
        return None
    parts = fk.split(".")
    if len(parts) == 2:
        return parts[0]
    if len(parts) == 3:
        return parts[1]
    return None


def _extract_fk_target(fk: str | None, current_schema: str) -> tuple[str, str, str] | None:
    if not fk:
        return None
    parts = fk.split(".")
    if len(parts) == 2:
        return current_schema, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None


def _generate_entity_row(
    entity: EntityDef,
    row_index: int,
    generated: dict[str, list[dict[str, Any]]],
    dictionary: DataDictionary,
    rng: random.Random,
    locale: str,
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for column in entity.columns:
        row[column.name] = _generate_column_value(
            entity=entity,
            column=column,
            row_index=row_index,
            row=row,
            generated=generated,
            dictionary=dictionary,
            rng=rng,
            locale=locale,
        )
    return row


def _generate_column_value(
    entity: EntityDef,
    column: Any,
    row_index: int,
    row: dict[str, Any],
    generated: dict[str, list[dict[str, Any]]],
    dictionary: DataDictionary,
    rng: random.Random,
    locale: str,
) -> Any:
    fk_target = _extract_fk_target(column.fk, entity.schema_name)
    if fk_target:
        schema_name, table_name, target_column = fk_target
        parent_rows = generated.get(f"{schema_name}.{table_name}", [])
        if parent_rows:
            return parent_rows[row_index % len(parent_rows)].get(target_column)

    if column.default == "true":
        return True
    if column.default == "false":
        return False
    if column.default == "now()":
        return f"2026-04-{(row_index % 28) + 1:02d}T10:00:00Z"
    if column.default and re.match(r"^'.*'$", str(column.default)):
        return str(column.default)[1:-1]

    logical_type = column.type.lower()
    col_name = column.name.lower()

    if "auto" in logical_type:
        return row_index + 1
    if logical_type.startswith("integer") or logical_type.startswith("long"):
        return row_index + 1
    if logical_type.startswith("decimal"):
        return round((row_index + 1) * 10.5, 2)
    if logical_type.startswith("boolean"):
        return row_index % 2 == 0
    if "timestamp" in logical_type:
        return f"2026-04-{(row_index % 28) + 1:02d}T10:00:00Z"
    if logical_type == "date":
        return f"2026-04-{(row_index % 28) + 1:02d}"
    if logical_type == "json":
        return {"seed": True, "table": entity.name, "row": row_index + 1}

    semantic = _semantic_string(col_name, row_index, locale, rng)
    if semantic is not None:
        return semantic

    if not column.nullable:
        return f"{entity.name}_{column.name}_{row_index + 1}"
    return None


def _semantic_string(col_name: str, row_index: int, locale: str, rng: random.Random) -> str | None:
    if "email" in col_name:
        return f"user{row_index + 1}@example.com"
    if "phone" in col_name or "mobile" in col_name:
        return f"+39 320 555 {1000 + row_index}"
    if "name" in col_name:
        names = ["Luna", "Orion", "Astra", "Milo", "Elia", "Nora", "Vega", "Dalia"]
        return f"{names[row_index % len(names)]} {row_index + 1}"
    if "status" in col_name:
        return ["ACTIVE", "PENDING", "DONE", "SUSPENDED"][row_index % 4]
    if "code" in col_name or col_name.endswith("_id"):
        return f"{col_name.upper()}_{row_index + 1:03d}"
    if "description" in col_name or "message" in col_name:
        return f"Sample {col_name.replace('_', ' ')} #{row_index + 1}"
    if "city" in col_name:
        return ["Rome", "Milan", "Turin", "Bologna"][row_index % 4]
    if "country" in col_name:
        return "Italy" if locale.startswith("it") else "USA"
    return None
