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


# --- Plan ---

class PlanMetadata(BaseModel):
    """Metadata for a plan file — who, when, what."""
    created_at: str  # ISO 8601
    dictionary_path: str
    dictionary_hash: str  # SHA-256 of dictionary file
    connection: str  # sanitized (no password)
    engine: str = "pg"
    schema_filter: str | None = None


class PlanResult(BaseModel):
    """A saved plan: desired-vs-actual diff with audit trail."""
    version: str = "1"
    metadata: PlanMetadata
    changes: list[SchemaChange]
    risk_level: RiskLevel
    plan_hash: str = ""  # SHA-256 of changes JSON (tamper detection)
    rollback_sql: str = ""
    summary: dict[str, int] = Field(default_factory=dict)  # counts by change_type


# --- Drift ---

class DriftType(str, Enum):
    EXTRA_TABLE = "extra_table"
    MISSING_TABLE = "missing_table"
    EXTRA_COLUMN = "extra_column"
    MISSING_COLUMN = "missing_column"
    TYPE_MISMATCH = "type_mismatch"
    MISSING_INDEX = "missing_index"
    EXTRA_INDEX = "extra_index"
    MISSING_RLS = "missing_rls"
    MISSING_MASKING = "missing_masking"


class DriftItem(BaseModel):
    """A single drift finding."""
    drift_type: DriftType
    object_name: str
    details: dict[str, Any] = Field(default_factory=dict)
    suggested_action: str = ""


class DriftReport(BaseModel):
    """Full drift detection report."""
    has_drift: bool = False
    items: list[DriftItem] = Field(default_factory=list)
    summary: dict[str, int] = Field(default_factory=dict)
    profile: str = "essential"
    engine: str = "pg"
    schema_filter: str | None = None


# --- API: Plan ---

class PlanRequest(BaseModel):
    connection_string: str
    dictionary: dict[str, Any]
    engine: str = "pg"
    schema_filter: str | None = None


class ApplyRequest(BaseModel):
    connection_string: str
    plan: PlanResult
    dry_run: bool = True


class ApplyResponse(BaseModel):
    applied: list[SchemaChange]
    rollback_sql: str
    dry_run: bool


# --- API: Drift Dictionary ---

class DriftDictionaryRequest(BaseModel):
    connection_string: str
    dictionary: dict[str, Any]
    engine: str = "pg"
    profile: str = "essential"
    schema_filter: str | None = None


# --- API: Compile ---

class CompileRequest(BaseModel):
    dictionary: dict[str, Any]
    engine: str = "pg"
    profile: str = "essential"
    schema_filter: str | None = None


class CompileResponse(BaseModel):
    engine: str
    profile: str
    entity_count: int
    files: list[dict[str, str]]
    index_count: int = 0
    fk_count: int = 0
    check_count: int = 0
    comment_count: int = 0


# --- API: Validate ---

class ValidateRequest(BaseModel):
    dictionary: dict[str, Any]
    suite: str | None = None
    schema_filter: str | None = None


class ValidateResponse(BaseModel):
    total_checks: int
    passed: int
    failed: int
    warnings: int
    all_passed: bool
    results: list[dict[str, Any]] = Field(default_factory=list)


# --- Health ---

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str


# --- Agentic Schema Observer ---

class AgentAskRequest(BaseModel):
    question: str
    dictionary: dict[str, Any]
    connection_string: str = ""
    model: str = "default"

class AgentAskResponse(BaseModel):
    answer: str
    metadata: dict[str, Any] = Field(default_factory=dict)

# --- Reverse Engineering ---

class ReverseEngineerRequest(BaseModel):
    connection_string: str
    schema_filter: str | None = None

class ReverseEngineerResponse(BaseModel):
    dictionary: dict[str, Any]

