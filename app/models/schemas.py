"""Pydantic models for DB-HALE-BOPP API requests and responses."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ChangeType(str, Enum):
    ADD_TABLE = "add_table"
    DROP_TABLE = "drop_table"
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    ALTER_COLUMN = "alter_column"
    ADD_INDEX = "add_index"
    DROP_INDEX = "drop_index"
    ADD_CONSTRAINT = "add_constraint"
    DROP_CONSTRAINT = "drop_constraint"


class SchemaChange(BaseModel):
    change_type: ChangeType
    object_name: str
    details: dict[str, Any] = Field(default_factory=dict)
    sql_up: str = ""
    sql_down: str = ""


# --- Diff ---

class DiffRequest(BaseModel):
    connection_string: str
    desired_schema: dict[str, Any]


class DiffResponse(BaseModel):
    changes: list[SchemaChange]
    risk_level: RiskLevel


# --- Deploy ---

class DeployRequest(BaseModel):
    connection_string: str
    changes: list[SchemaChange]
    dry_run: bool = True


class DeployResponse(BaseModel):
    applied: list[SchemaChange]
    rollback_sql: str


# --- Drift ---

class DriftCheckRequest(BaseModel):
    connection_string: str
    baseline_id: str = ""


class DriftCheckResponse(BaseModel):
    drifted: bool
    diffs: list[SchemaChange]


# --- Maetel ---

class MaetelRequest(BaseModel):
    connection_string: str
    format: str = "mermaid"
    schema_name: str | None = None


class MaetelResponse(BaseModel):
    format: str
    content: str
    stats: dict[str, Any] = Field(default_factory=dict)


# --- Health ---

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
