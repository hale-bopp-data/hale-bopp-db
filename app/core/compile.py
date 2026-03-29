"""hb compile — Dictionary Reader + DDL Compiler.

Reads db-data-dictionary.json (or an intent JSON) and produces
idempotent DDL for the target engine.

Phase 1: PostgreSQL compiler (PBI #542).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Pydantic models — Dictionary Reader
# ---------------------------------------------------------------------------

class ColumnDef(BaseModel):
    name: str
    type: str
    nullable: bool = True
    default: str | None = None
    description: str | None = None
    description_nonna: str | None = None
    check: str | None = None
    fk: str | None = None
    on_delete: str | None = None
    pii: bool = False
    computed: str | None = None
    note: str | None = None
    pk: bool = False  # used in intent JSON

    model_config = {"extra": "ignore"}


class IndexDef(BaseModel):
    name: str
    columns: list[str]
    unique: bool = False
    desc: bool = False
    desc_last: bool = False

    model_config = {"extra": "ignore"}


class ConstraintDef(BaseModel):
    name: str
    type: str  # UNIQUE, CHECK, etc.
    columns: list[str] = Field(default_factory=list)
    expression: str | None = None

    model_config = {"extra": "ignore"}


class EntityDef(BaseModel):
    id: str = ""
    name: str
    schema_name: str = Field("public", alias="schema")
    type: str = "TABLE"  # DIM, FACT, LOG, CONFIG, META, BRIDGE
    description: str | None = None
    description_nonna: str | None = None
    multi_tenant: Any = False
    security: dict[str, Any] = Field(default_factory=dict)
    status: dict[str, Any] | str = Field(default_factory=dict)
    pk: dict[str, Any] | None = None
    business_key: dict[str, Any] | None = None
    columns: list[ColumnDef]
    indexes: list[IndexDef] = Field(default_factory=list)
    constraints: list[ConstraintDef] = Field(default_factory=list)
    retention: dict[str, Any] | None = None

    model_config = {"extra": "ignore", "populate_by_name": True}

    @field_validator("id", mode="before")
    @classmethod
    def default_id(cls, v: Any, info: Any) -> str:
        return v or ""


class DataDictionary(BaseModel):
    """Top-level dictionary model — only the fields we need for compile."""
    type_map: dict[str, dict[str, str]]
    default_map: dict[str, dict[str, str | None]]
    security_profiles: dict[str, Any] = Field(default_factory=dict)
    standard_columns: list[dict[str, Any]] = Field(default_factory=list)
    schemas: list[dict[str, Any]] = Field(default_factory=list)
    entities: list[EntityDef]
    relationships: list[dict[str, Any]] = Field(default_factory=list)
    name_mapping: list[dict[str, Any]] = Field(default_factory=list)
    rls_map: dict[str, dict[str, str]] = Field(default_factory=dict)
    masking_map: dict[str, dict[str, str]] = Field(default_factory=dict)
    redis_patterns: list[dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


# ---------------------------------------------------------------------------
# Dictionary Reader
# ---------------------------------------------------------------------------

def load_dictionary(path: str | Path) -> DataDictionary:
    """Load and validate a data dictionary JSON file."""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return DataDictionary.model_validate(raw)


# ---------------------------------------------------------------------------
# Type & Default resolvers
# ---------------------------------------------------------------------------

def resolve_type(logical_type: str, type_map: dict[str, dict[str, str]], engine: str) -> str:
    """Translate a logical type to an engine-specific type.

    Handles parameterised types like ``string(100)`` and ``decimal(10,6)``.
    """
    # Direct match first
    if logical_type in type_map:
        return type_map[logical_type].get(engine, logical_type.upper())

    # Parameterised match: string(100) → string(n), decimal(10,6) → decimal(p,s)
    m = re.match(r"^(\w+)\((.+)\)$", logical_type)
    if m:
        base, params = m.group(1), m.group(2)
        parts = [p.strip() for p in params.split(",")]

        # Try template keys in order of specificity
        if len(parts) == 2:
            template_key = f"{base}(p,s)"
        else:
            template_key = f"{base}(n)"

        if template_key in type_map:
            result = type_map[template_key].get(engine, logical_type.upper())
            if len(parts) == 2:
                result = result.replace("{p}", parts[0]).replace("{s}", parts[1])
            else:
                result = result.replace("{n}", parts[0])
            return result

    # Fallback — return as-is uppercased
    return logical_type.upper()


def resolve_default(logical_default: str, default_map: dict[str, dict[str, str | None]], engine: str) -> str | None:
    """Translate a logical default to an engine-specific default."""
    if logical_default in default_map:
        return default_map[logical_default].get(engine)

    # Literal strings (e.g. "'system'", "'MANUAL'") pass through
    return logical_default


# ---------------------------------------------------------------------------
# PostgreSQL DDL Compiler
# ---------------------------------------------------------------------------

class CompileResult(BaseModel):
    """Result of a compile run."""
    engine: str
    profile: str
    files: list[dict[str, str]] = Field(default_factory=list)  # [{name, content}]
    entity_count: int = 0
    index_count: int = 0
    fk_count: int = 0
    check_count: int = 0
    comment_count: int = 0
    security_file_count: int = 0


def compile_pg(
    dictionary: DataDictionary,
    profile: str = "essential",
    schema_filter: str | None = None,
) -> CompileResult:
    """Compile dictionary entities into PostgreSQL DDL.

    Returns a CompileResult with numbered SQL file contents.
    """
    type_map = dictionary.type_map
    default_map = dictionary.default_map
    engine = "pg"

    entities = dictionary.entities
    if schema_filter:
        entities = [e for e in entities if e.schema_name == schema_filter]

    # Collect all schema names
    schema_names = sorted({e.schema_name for e in entities})

    # --- 001: CREATE SCHEMA ---
    schema_sql_parts: list[str] = []
    for s in schema_names:
        schema_sql_parts.append(f"CREATE SCHEMA IF NOT EXISTS {s};")
    schema_sql = "\n".join(schema_sql_parts)

    # Build entity→schema lookup for cross-schema FK resolution
    entity_schema_map: dict[str, str] = {}
    for e in dictionary.entities:
        entity_schema_map[e.name] = e.schema_name
        if e.id:
            entity_schema_map[e.id] = e.schema_name

    # --- 002: CREATE TABLE ---
    table_sql_parts: list[str] = []
    all_fk_parts: list[str] = []
    all_index_parts: list[str] = []
    all_comment_parts: list[str] = []
    check_count = 0
    fk_count = 0

    for entity in entities:
        tbl = _compile_table(entity, type_map, default_map, engine, entity_schema_map)
        table_sql_parts.append(tbl["create_table"])

        all_fk_parts.extend(tbl["fk_statements"])
        fk_count += len(tbl["fk_statements"])

        all_index_parts.extend(tbl["index_statements"])
        all_comment_parts.extend(tbl["comment_statements"])
        check_count += tbl["check_count"]

    tables_sql = "\n\n".join(table_sql_parts)

    # --- 003: CREATE INDEX ---
    indexes_sql = "\n".join(all_index_parts)

    # --- 004: ALTER TABLE (FK) ---
    fk_sql = "\n".join(all_fk_parts)

    # --- 005: COMMENT ON ---
    comments_sql = "\n".join(all_comment_parts)

    # Build file list
    files: list[dict[str, str]] = []
    if schema_sql.strip():
        files.append({"name": "001_schemas.sql", "content": _header("Schemas") + schema_sql + "\n"})
    if tables_sql.strip():
        files.append({"name": "002_tables.sql", "content": _header("Tables") + tables_sql + "\n"})
    if indexes_sql.strip():
        files.append({"name": "003_indexes.sql", "content": _header("Indexes") + indexes_sql + "\n"})
    if fk_sql.strip():
        files.append({"name": "004_foreign_keys.sql", "content": _header("Foreign Keys") + fk_sql + "\n"})
    if comments_sql.strip():
        files.append({"name": "005_comments.sql", "content": _header("Comments") + comments_sql + "\n"})

    return CompileResult(
        engine="pg",
        profile=profile,
        files=files,
        entity_count=len(entities),
        index_count=len(all_index_parts),
        fk_count=fk_count,
        check_count=check_count,
        comment_count=len(all_comment_parts),
    )


def _header(section: str) -> str:
    """SQL file header comment."""
    return (
        f"-- =============================================================\n"
        f"-- {section}\n"
        f"-- Generated by hale-bopp-db compile (PostgreSQL)\n"
        f"-- =============================================================\n\n"
    )


def _compile_table(
    entity: EntityDef,
    type_map: dict[str, dict[str, str]],
    default_map: dict[str, dict[str, str | None]],
    engine: str,
    entity_schema_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Compile a single entity into DDL components."""
    schema = entity.schema_name
    table = entity.name
    qualified = f"{schema}.{table}"

    col_lines: list[str] = []
    fk_statements: list[str] = []
    comment_statements: list[str] = []
    check_count = 0

    # Determine PK columns
    pk_columns: list[str] = []
    if entity.pk:
        pk_columns = entity.pk.get("columns", [])

    # Also check for pk=true in column definitions (intent JSON style)
    for col in entity.columns:
        if col.pk and col.name not in pk_columns:
            pk_columns.append(col.name)

    for col in entity.columns:
        line = _compile_column(col, pk_columns, type_map, default_map, engine)
        col_lines.append(line)

        # CHECK constraint inline
        if col.check:
            check_count += 1

        # FK — collected for ALTER TABLE
        if col.fk:
            fk_stmt = _compile_fk(schema, table, col, entity_schema_map)
            if fk_stmt:
                fk_statements.append(fk_stmt)

        # Column comment
        if col.description:
            comment_statements.append(
                f"COMMENT ON COLUMN {qualified}.{col.name} IS {_sql_literal(col.description)};"
            )

    # Composite PK (if not single-column handled inline)
    pk_clause = ""
    if len(pk_columns) > 1:
        pk_clause = f",\n    CONSTRAINT pk_{table} PRIMARY KEY ({', '.join(pk_columns)})"

    # Table-level constraints (UNIQUE, etc.)
    constraint_clauses: list[str] = []
    for c in entity.constraints:
        if c.type.upper() == "UNIQUE" and c.columns:
            constraint_clauses.append(
                f"    CONSTRAINT {c.name} UNIQUE ({', '.join(c.columns)})"
            )

    # Business key as UNIQUE constraint (if not already in indexes)
    # Skipped — business_key unique is usually covered by an explicit index

    col_block = ",\n".join(f"    {line}" for line in col_lines)
    if pk_clause:
        col_block += pk_clause
    if constraint_clauses:
        col_block += ",\n" + ",\n".join(constraint_clauses)

    create_table = (
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
        f"{col_block}\n"
        f");"
    )

    # Table comment
    if entity.description:
        comment_statements.insert(
            0, f"COMMENT ON TABLE {qualified} IS {_sql_literal(entity.description)};"
        )

    # Indexes
    index_statements = _compile_indexes(entity, qualified)

    return {
        "create_table": create_table,
        "fk_statements": fk_statements,
        "index_statements": index_statements,
        "comment_statements": comment_statements,
        "check_count": check_count,
    }


def _compile_column(
    col: ColumnDef,
    pk_columns: list[str],
    type_map: dict[str, dict[str, str]],
    default_map: dict[str, dict[str, str | None]],
    engine: str,
) -> str:
    """Compile a single column definition."""
    pg_type = resolve_type(col.type, type_map, engine)

    parts = [col.name, pg_type]

    # PRIMARY KEY (only for single-column PKs)
    is_pk = col.name in pk_columns
    if is_pk and len(pk_columns) == 1:
        parts.append("PRIMARY KEY")

    # NOT NULL (skip for PKs — implied, and skip for BIGSERIAL PKs)
    if not col.nullable and not is_pk:
        parts.append("NOT NULL")

    # DEFAULT
    if col.default is not None:
        resolved = resolve_default(col.default, default_map, engine)
        if resolved is not None:
            parts.append(f"DEFAULT {resolved}")

    # CHECK constraint (inline)
    if col.check:
        check_expr = _translate_check_pg(col.name, col.check)
        parts.append(f"CHECK ({check_expr})")

    return " ".join(parts)


def _translate_check_pg(col_name: str, check_expr: str) -> str:
    """Translate a logical CHECK expression to PostgreSQL dialect.

    The dictionary uses shorthand like:
      - ``IN ('A', 'B', 'C')`` → ``col_name IN ('A', 'B', 'C')``
      - ``BETWEEN 1 AND 3`` → ``col_name BETWEEN 1 AND 3``
      - Full expressions with column name pass through.
    """
    stripped = check_expr.strip()

    # If the expression already contains the column name, pass through
    if col_name in stripped:
        return stripped

    # Shorthand: starts with IN, BETWEEN, >, <, >=, <=, !=, =
    if re.match(r"^(IN|BETWEEN|>=?|<=?|!=|=)\s", stripped, re.IGNORECASE):
        return f"{col_name} {stripped}"

    # Fallback — return as-is
    return stripped


def _compile_fk(
    schema: str,
    table: str,
    col: ColumnDef,
    entity_schema_map: dict[str, str] | None = None,
) -> str | None:
    """Compile a FK column into an ALTER TABLE statement."""
    if not col.fk:
        return None

    # FK format: "target_table.target_column" or "schema.target_table.target_column"
    parts = col.fk.split(".")
    if len(parts) == 2:
        ref_table, ref_col = parts
        # Resolve the correct schema for the referenced table
        if entity_schema_map and ref_table in entity_schema_map:
            ref_schema = entity_schema_map[ref_table]
        else:
            ref_schema = schema  # fallback to same schema
        ref_qualified = f"{ref_schema}.{ref_table}"
    elif len(parts) == 3:
        ref_schema, ref_table, ref_col = parts
        ref_qualified = f"{ref_schema}.{ref_table}"
    else:
        return None

    fk_name = f"fk_{table}_{col.name}"
    on_delete = f" ON DELETE {col.on_delete.upper()}" if col.on_delete else ""

    return (
        f"ALTER TABLE {schema}.{table} "
        f"ADD CONSTRAINT {fk_name} "
        f"FOREIGN KEY ({col.name}) REFERENCES {ref_qualified}({ref_col}){on_delete};"
    )


def _compile_indexes(entity: EntityDef, qualified: str) -> list[str]:
    """Compile index definitions for an entity."""
    stmts: list[str] = []
    for idx in entity.indexes:
        unique = "UNIQUE " if idx.unique else ""
        cols = ", ".join(idx.columns)
        stmts.append(
            f"CREATE {unique}INDEX IF NOT EXISTS {idx.name} ON {qualified} ({cols});"
        )
    return stmts


def _sql_literal(value: str) -> str:
    """Escape a string for use as a SQL literal."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


# ---------------------------------------------------------------------------
# SQL Server (MSSQL) DDL Compiler
# ---------------------------------------------------------------------------

def compile_mssql(
    dictionary: DataDictionary,
    profile: str = "essential",
    schema_filter: str | None = None,
) -> CompileResult:
    """Compile dictionary entities into SQL Server (T-SQL) DDL."""
    type_map = dictionary.type_map
    default_map = dictionary.default_map
    engine = "mssql"

    entities = dictionary.entities
    if schema_filter:
        entities = [e for e in entities if e.schema_name == schema_filter]

    schema_names = sorted({e.schema_name for e in entities})

    # Build entity→schema lookup
    entity_schema_map: dict[str, str] = {}
    for e in dictionary.entities:
        entity_schema_map[e.name] = e.schema_name
        if e.id:
            entity_schema_map[e.id] = e.schema_name

    # --- 001: CREATE SCHEMA (idempotent T-SQL) ---
    schema_parts: list[str] = []
    for s in schema_names:
        schema_parts.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{s}')\n"
            f"    EXEC('CREATE SCHEMA [{s}]');\nGO"
        )
    schema_sql = "\n\n".join(schema_parts)

    # --- 002: CREATE TABLE ---
    table_parts: list[str] = []
    all_fk_parts: list[str] = []
    all_index_parts: list[str] = []
    all_comment_parts: list[str] = []
    check_count = 0
    fk_count = 0

    for entity in entities:
        tbl = _compile_table_mssql(entity, type_map, default_map, engine, entity_schema_map)
        table_parts.append(tbl["create_table"])
        all_fk_parts.extend(tbl["fk_statements"])
        fk_count += len(tbl["fk_statements"])
        all_index_parts.extend(tbl["index_statements"])
        all_comment_parts.extend(tbl["comment_statements"])
        check_count += tbl["check_count"]

    tables_sql = "\n\n".join(table_parts)
    indexes_sql = "\n".join(all_index_parts)
    fk_sql = "\n".join(all_fk_parts)
    comments_sql = "\n".join(all_comment_parts)

    files: list[dict[str, str]] = []
    if schema_sql.strip():
        files.append({"name": "001_schemas.sql", "content": _header_mssql("Schemas") + schema_sql + "\n"})
    if tables_sql.strip():
        files.append({"name": "002_tables.sql", "content": _header_mssql("Tables") + tables_sql + "\n"})
    if indexes_sql.strip():
        files.append({"name": "003_indexes.sql", "content": _header_mssql("Indexes") + indexes_sql + "\n"})
    if fk_sql.strip():
        files.append({"name": "004_foreign_keys.sql", "content": _header_mssql("Foreign Keys") + fk_sql + "\n"})
    if comments_sql.strip():
        files.append({"name": "005_comments.sql", "content": _header_mssql("Comments") + comments_sql + "\n"})

    return CompileResult(
        engine="mssql",
        profile=profile,
        files=files,
        entity_count=len(entities),
        index_count=len(all_index_parts),
        fk_count=fk_count,
        check_count=check_count,
        comment_count=len(all_comment_parts),
    )


def _header_mssql(section: str) -> str:
    return (
        f"-- =============================================================\n"
        f"-- {section}\n"
        f"-- Generated by hale-bopp-db compile (SQL Server)\n"
        f"-- =============================================================\n\n"
    )


def _compile_table_mssql(
    entity: EntityDef,
    type_map: dict[str, dict[str, str]],
    default_map: dict[str, dict[str, str | None]],
    engine: str,
    entity_schema_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Compile a single entity into T-SQL DDL components."""
    schema = entity.schema_name
    table = entity.name
    qualified = f"[{schema}].[{table}]"

    col_lines: list[str] = []
    fk_statements: list[str] = []
    comment_statements: list[str] = []
    check_count = 0

    pk_columns: list[str] = []
    if entity.pk:
        pk_columns = entity.pk.get("columns", [])
    for col in entity.columns:
        if col.pk and col.name not in pk_columns:
            pk_columns.append(col.name)

    for col in entity.columns:
        line = _compile_column_mssql(col, pk_columns, type_map, default_map, engine)
        col_lines.append(line)

        if col.check:
            check_count += 1

        if col.fk:
            fk_stmt = _compile_fk_mssql(schema, table, col, entity_schema_map)
            if fk_stmt:
                fk_statements.append(fk_stmt)

        if col.description:
            comment_statements.append(_mssql_extended_property(
                schema, table, col.name, col.description
            ))

    # PK constraint
    pk_clause = ""
    if pk_columns:
        pk_cols_str = ", ".join(f"[{c}]" for c in pk_columns)
        pk_clause = f",\n    CONSTRAINT [PK_{table}] PRIMARY KEY ({pk_cols_str})"

    col_block = ",\n".join(f"    {line}" for line in col_lines)
    if pk_clause:
        col_block += pk_clause

    # T-SQL idempotent: IF OBJECT_ID IS NULL
    create_table = (
        f"IF OBJECT_ID(N'{schema}.{table}', N'U') IS NULL\n"
        f"CREATE TABLE {qualified} (\n"
        f"{col_block}\n"
        f");\nGO"
    )

    # Table description via extended property
    if entity.description:
        comment_statements.insert(0, _mssql_extended_property(
            schema, table, None, entity.description
        ))

    # Indexes
    index_statements = _compile_indexes_mssql(entity, schema, table)

    return {
        "create_table": create_table,
        "fk_statements": fk_statements,
        "index_statements": index_statements,
        "comment_statements": comment_statements,
        "check_count": check_count,
    }


def _compile_column_mssql(
    col: Any,
    pk_columns: list[str],
    type_map: dict[str, dict[str, str]],
    default_map: dict[str, dict[str, str | None]],
    engine: str,
) -> str:
    """Compile a single column definition for T-SQL."""
    mssql_type = resolve_type(col.type, type_map, engine)

    parts = [f"[{col.name}]", mssql_type]

    # NOT NULL
    if not col.nullable:
        parts.append("NOT NULL")
    else:
        parts.append("NULL")

    # DEFAULT
    if col.default is not None:
        resolved = resolve_default(col.default, default_map, engine)
        if resolved is not None:
            parts.append(f"DEFAULT {resolved}")

    # CHECK constraint (inline)
    if col.check:
        check_expr = col.check.strip()
        if col.name not in check_expr:
            check_expr = f"[{col.name}] {check_expr}"
        parts.append(f"CHECK ({check_expr})")

    # Computed column
    if col.computed:
        return f"[{col.name}] AS ({col.computed})"

    return " ".join(parts)


def _compile_fk_mssql(
    schema: str,
    table: str,
    col: Any,
    entity_schema_map: dict[str, str] | None = None,
) -> str | None:
    """Compile a FK column into T-SQL ALTER TABLE."""
    if not col.fk:
        return None

    parts = col.fk.split(".")
    if len(parts) == 2:
        ref_table, ref_col = parts
        ref_schema = (entity_schema_map or {}).get(ref_table, schema)
    elif len(parts) == 3:
        ref_schema, ref_table, ref_col = parts
    else:
        return None

    fk_name = f"FK_{table}_{col.name}"
    on_delete = f" ON DELETE {col.on_delete.upper()}" if col.on_delete else ""

    return (
        f"IF OBJECT_ID(N'{fk_name}', N'F') IS NULL\n"
        f"ALTER TABLE [{schema}].[{table}] "
        f"ADD CONSTRAINT [{fk_name}] "
        f"FOREIGN KEY ([{col.name}]) REFERENCES [{ref_schema}].[{ref_table}]([{ref_col}]){on_delete};\nGO"
    )


def _compile_indexes_mssql(entity: EntityDef, schema: str, table: str) -> list[str]:
    """Compile index definitions for T-SQL."""
    stmts: list[str] = []
    for idx in entity.indexes:
        unique = "UNIQUE " if idx.unique else ""
        cols = ", ".join(f"[{c}]" for c in idx.columns)
        stmts.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{idx.name}' "
            f"AND object_id = OBJECT_ID(N'{schema}.{table}'))\n"
            f"CREATE {unique}INDEX [{idx.name}] ON [{schema}].[{table}] ({cols});\nGO"
        )
    return stmts


def _mssql_extended_property(
    schema: str, table: str, column: str | None, description: str
) -> str:
    """Generate sp_addextendedproperty for table/column descriptions."""
    escaped = description.replace("'", "''")
    if column:
        return (
            f"IF NOT EXISTS (SELECT 1 FROM fn_listextendedproperty(N'MS_Description', "
            f"N'SCHEMA', N'{schema}', N'TABLE', N'{table}', N'COLUMN', N'{column}'))\n"
            f"EXEC sp_addextendedproperty "
            f"@name = N'MS_Description', @value = N'{escaped}', "
            f"@level0type = N'SCHEMA', @level0name = N'{schema}', "
            f"@level1type = N'TABLE', @level1name = N'{table}', "
            f"@level2type = N'COLUMN', @level2name = N'{column}';\nGO"
        )
    return (
        f"IF NOT EXISTS (SELECT 1 FROM fn_listextendedproperty(N'MS_Description', "
        f"N'SCHEMA', N'{schema}', N'TABLE', N'{table}', NULL, NULL))\n"
        f"EXEC sp_addextendedproperty "
        f"@name = N'MS_Description', @value = N'{escaped}', "
        f"@level0type = N'SCHEMA', @level0name = N'{schema}', "
        f"@level1type = N'TABLE', @level1name = N'{table}';\nGO"
    )


# ---------------------------------------------------------------------------
# Oracle DDL Compiler
# ---------------------------------------------------------------------------

def compile_oracle(
    dictionary: DataDictionary,
    profile: str = "essential",
    schema_filter: str | None = None,
) -> CompileResult:
    """Compile dictionary entities into Oracle PL/SQL DDL."""
    type_map = dictionary.type_map
    default_map = dictionary.default_map
    engine = "oracle"

    entities = dictionary.entities
    if schema_filter:
        entities = [e for e in entities if e.schema_name == schema_filter]

    schema_names = sorted({e.schema_name for e in entities})

    entity_schema_map: dict[str, str] = {}
    for e in dictionary.entities:
        entity_schema_map[e.name] = e.schema_name
        if e.id:
            entity_schema_map[e.id] = e.schema_name

    # --- 001: CREATE USER (schema) ---
    schema_parts: list[str] = []
    for s in schema_names:
        schema_parts.append(
            f"-- Schema/user: {s}\n"
            f"BEGIN\n"
            f"  EXECUTE IMMEDIATE 'CREATE USER {s} IDENTIFIED BY changeme "
            f"DEFAULT TABLESPACE users QUOTA UNLIMITED ON users';\n"
            f"EXCEPTION WHEN OTHERS THEN\n"
            f"  IF SQLCODE != -1920 THEN RAISE; END IF; -- ORA-01920: user already exists\n"
            f"END;\n/"
        )
    schema_sql = "\n\n".join(schema_parts)

    # --- 002: CREATE TABLE ---
    table_parts: list[str] = []
    all_fk_parts: list[str] = []
    all_index_parts: list[str] = []
    all_comment_parts: list[str] = []
    check_count = 0
    fk_count = 0

    for entity in entities:
        tbl = _compile_table_oracle(entity, type_map, default_map, engine, entity_schema_map)
        table_parts.append(tbl["create_table"])
        all_fk_parts.extend(tbl["fk_statements"])
        fk_count += len(tbl["fk_statements"])
        all_index_parts.extend(tbl["index_statements"])
        all_comment_parts.extend(tbl["comment_statements"])
        check_count += tbl["check_count"]

    tables_sql = "\n\n".join(table_parts)
    indexes_sql = "\n\n".join(all_index_parts)
    fk_sql = "\n\n".join(all_fk_parts)
    comments_sql = "\n".join(all_comment_parts)

    files: list[dict[str, str]] = []
    if schema_sql.strip():
        files.append({"name": "001_schemas.sql", "content": _header_oracle("Schemas") + schema_sql + "\n"})
    if tables_sql.strip():
        files.append({"name": "002_tables.sql", "content": _header_oracle("Tables") + tables_sql + "\n"})
    if indexes_sql.strip():
        files.append({"name": "003_indexes.sql", "content": _header_oracle("Indexes") + indexes_sql + "\n"})
    if fk_sql.strip():
        files.append({"name": "004_foreign_keys.sql", "content": _header_oracle("Foreign Keys") + fk_sql + "\n"})
    if comments_sql.strip():
        files.append({"name": "005_comments.sql", "content": _header_oracle("Comments") + comments_sql + "\n"})

    return CompileResult(
        engine="oracle",
        profile=profile,
        files=files,
        entity_count=len(entities),
        index_count=len(all_index_parts),
        fk_count=fk_count,
        check_count=check_count,
        comment_count=len(all_comment_parts),
    )


def _header_oracle(section: str) -> str:
    return (
        f"-- =============================================================\n"
        f"-- {section}\n"
        f"-- Generated by hale-bopp-db compile (Oracle)\n"
        f"-- =============================================================\n\n"
    )


def _compile_table_oracle(
    entity: EntityDef,
    type_map: dict[str, dict[str, str]],
    default_map: dict[str, dict[str, str | None]],
    engine: str,
    entity_schema_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Compile a single entity into Oracle PL/SQL DDL."""
    schema = entity.schema_name
    table = entity.name
    qualified = f"{schema}.{table}"

    col_lines: list[str] = []
    fk_statements: list[str] = []
    comment_statements: list[str] = []
    check_count = 0

    pk_columns: list[str] = []
    if entity.pk:
        pk_columns = entity.pk.get("columns", [])
    for col in entity.columns:
        if col.pk and col.name not in pk_columns:
            pk_columns.append(col.name)

    for col in entity.columns:
        line = _compile_column_oracle(col, pk_columns, type_map, default_map, engine)
        col_lines.append(line)

        if col.check:
            check_count += 1

        if col.fk:
            fk_stmt = _compile_fk_oracle(schema, table, col, entity_schema_map)
            if fk_stmt:
                fk_statements.append(fk_stmt)

        if col.description:
            escaped = col.description.replace("'", "''")
            comment_statements.append(
                f"COMMENT ON COLUMN {qualified}.{col.name} IS '{escaped}';"
            )

    # PK constraint
    pk_clause = ""
    if pk_columns:
        pk_cols_str = ", ".join(pk_columns)
        pk_clause = f",\n    CONSTRAINT pk_{table} PRIMARY KEY ({pk_cols_str})"

    col_block = ",\n".join(f"    {line}" for line in col_lines)
    if pk_clause:
        col_block += pk_clause

    # Oracle idempotent: EXECUTE IMMEDIATE + EXCEPTION
    create_table = (
        f"BEGIN\n"
        f"  EXECUTE IMMEDIATE '\n"
        f"    CREATE TABLE {qualified} (\n"
        f"{col_block}\n"
        f"    )';\n"
        f"EXCEPTION WHEN OTHERS THEN\n"
        f"  IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used\n"
        f"END;\n/"
    )

    # Table comment
    if entity.description:
        escaped = entity.description.replace("'", "''")
        comment_statements.insert(0, f"COMMENT ON TABLE {qualified} IS '{escaped}';")

    # Indexes
    index_statements = _compile_indexes_oracle(entity, schema, table)

    return {
        "create_table": create_table,
        "fk_statements": fk_statements,
        "index_statements": index_statements,
        "comment_statements": comment_statements,
        "check_count": check_count,
    }


def _compile_column_oracle(
    col: Any,
    pk_columns: list[str],
    type_map: dict[str, dict[str, str]],
    default_map: dict[str, dict[str, str | None]],
    engine: str,
) -> str:
    """Compile a single column definition for Oracle."""
    ora_type = resolve_type(col.type, type_map, engine)

    parts = [col.name, ora_type]

    # GENERATED ALWAYS AS IDENTITY for auto columns
    if "GENERATED" in ora_type and "IDENTITY" in ora_type:
        # Type already includes IDENTITY clause — just need NOT NULL
        if not col.nullable:
            parts.append("NOT NULL")
    else:
        # DEFAULT
        if col.default is not None:
            resolved = resolve_default(col.default, default_map, engine)
            if resolved is not None:
                parts.append(f"DEFAULT {resolved}")

        # NOT NULL
        if not col.nullable:
            parts.append("NOT NULL")

    # CHECK constraint (inline)
    if col.check:
        check_expr = col.check.strip()
        if col.name not in check_expr:
            check_expr = f"{col.name} {check_expr}"
        parts.append(f"CHECK ({check_expr})")

    # Computed column (virtual)
    if col.computed:
        return f"{col.name} GENERATED ALWAYS AS ({col.computed}) VIRTUAL"

    return " ".join(parts)


def _compile_fk_oracle(
    schema: str,
    table: str,
    col: Any,
    entity_schema_map: dict[str, str] | None = None,
) -> str | None:
    """Compile a FK column into Oracle ALTER TABLE."""
    if not col.fk:
        return None

    parts = col.fk.split(".")
    if len(parts) == 2:
        ref_table, ref_col = parts
        ref_schema = (entity_schema_map or {}).get(ref_table, schema)
    elif len(parts) == 3:
        ref_schema, ref_table, ref_col = parts
    else:
        return None

    fk_name = f"fk_{table}_{col.name}"
    on_delete = f" ON DELETE {col.on_delete.upper()}" if col.on_delete else ""

    return (
        f"BEGIN\n"
        f"  EXECUTE IMMEDIATE '\n"
        f"    ALTER TABLE {schema}.{table} "
        f"ADD CONSTRAINT {fk_name} "
        f"FOREIGN KEY ({col.name}) REFERENCES {ref_schema}.{ref_table}({ref_col}){on_delete}';\n"
        f"EXCEPTION WHEN OTHERS THEN\n"
        f"  IF SQLCODE != -2275 THEN RAISE; END IF; -- ORA-02275: constraint already exists\n"
        f"END;\n/"
    )


def _compile_indexes_oracle(entity: EntityDef, schema: str, table: str) -> list[str]:
    """Compile index definitions for Oracle."""
    stmts: list[str] = []
    for idx in entity.indexes:
        unique = "UNIQUE " if idx.unique else ""
        cols = ", ".join(idx.columns)
        stmts.append(
            f"BEGIN\n"
            f"  EXECUTE IMMEDIATE 'CREATE {unique}INDEX {idx.name} "
            f"ON {schema}.{table} ({cols})';\n"
            f"EXCEPTION WHEN OTHERS THEN\n"
            f"  IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used\n"
            f"END;\n/"
        )
    return stmts


# ---------------------------------------------------------------------------
# Public API — write files to disk
# ---------------------------------------------------------------------------

def compile_and_write(
    dictionary: DataDictionary,
    engine: str,
    profile: str,
    output_dir: str | Path,
    schema_filter: str | None = None,
) -> CompileResult:
    """Compile dictionary and write SQL files to output directory."""
    if engine == "mssql":
        result = compile_mssql(dictionary, profile=profile, schema_filter=schema_filter)
    elif engine == "oracle":
        result = compile_oracle(dictionary, profile=profile, schema_filter=schema_filter)
    elif engine == "pg":
        result = compile_pg(dictionary, profile=profile, schema_filter=schema_filter)
    else:
        raise ValueError(f"Engine '{engine}' not yet supported. Available: pg, mssql, oracle")

    # Security DDL (Standard/Enterprise profiles) — PG only for now
    if engine == "pg" and profile in ("standard", "enterprise"):
        from app.core.security import generate_security_pg
        sec = generate_security_pg(dictionary, profile=profile, schema_filter=schema_filter)
        sec_files = sec.to_files()
        result.files.extend(sec_files)
        result.security_file_count = len(sec_files)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    for f in result.files:
        (out / f["name"]).write_text(f["content"], encoding="utf-8")

    return result
