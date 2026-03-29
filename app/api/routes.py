"""DB-HALE-BOPP REST API routes."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from fastapi import APIRouter

from app.core.compile import DataDictionary, compile_and_write, load_dictionary
from app.core.deploy import deploy_changes
from app.core.diff import compute_diff
from app.core.drift_detect import detect_drift_from_schemas
from app.core.introspect import introspect_schema
from app.core.maetel import to_json as maetel_to_json
from app.core.maetel import to_mermaid as maetel_to_mermaid
from app.core.plan import (
    apply_plan,
    create_plan,
    dictionary_to_desired,
    _changes_hash,
    _sanitize_conn,
)
from app.core.validate import get_suite_checks, validate_dictionary
from app.models.schemas import (
    ApplyRequest,
    ApplyResponse,
    CompileRequest,
    CompileResponse,
    DeployRequest,
    DeployResponse,
    DiffRequest,
    DiffResponse,
    DriftCheckRequest,
    DriftCheckResponse,
    DriftDictionaryRequest,
    DriftReport,
    HealthResponse,
    MaetelRequest,
    MaetelResponse,
    PlanRequest,
    PlanResult,
    ValidateRequest,
    ValidateResponse,
)

router = APIRouter(prefix="/api/v1")

VERSION = "0.2.0"

# Default dictionary path — can be overridden with env var HB_DICTIONARY_PATH
import os
_DEFAULT_DICT = Path(__file__).resolve().parent.parent.parent / "data" / "db-data-dictionary.json"
_DICT_PATH = Path(os.environ.get("HB_DICTIONARY_PATH", str(_DEFAULT_DICT)))


# --- Dictionary (serve the source-of-truth JSON) ---

@router.get("/dictionary")
def get_dictionary():
    """Serve the data dictionary JSON so the UI can load it automatically."""
    if not _DICT_PATH.exists():
        from fastapi import HTTPException
        raise HTTPException(404, f"Dictionary not found: {_DICT_PATH}")
    return json.loads(_DICT_PATH.read_text(encoding="utf-8"))


# --- Diff (legacy) ---

@router.post("/schema/diff", response_model=DiffResponse)
def schema_diff(req: DiffRequest):
    actual = introspect_schema(req.connection_string)
    changes, risk = compute_diff(actual, req.desired_schema)
    return DiffResponse(changes=changes, risk_level=risk)


# --- Deploy (legacy) ---

@router.post("/schema/deploy", response_model=DeployResponse)
def schema_deploy(req: DeployRequest):
    applied, rollback_sql = deploy_changes(
        req.connection_string, req.changes, req.dry_run
    )
    return DeployResponse(applied=applied, rollback_sql=rollback_sql)


# --- Plan ---

@router.post("/plan", response_model=PlanResult)
def plan(req: PlanRequest):
    """Create a plan by comparing dictionary (desired) vs live DB (actual)."""
    dd = DataDictionary.model_validate(req.dictionary)
    desired = dictionary_to_desired(dd, engine=req.engine, schema_filter=req.schema_filter)
    actual = introspect_schema(req.connection_string, schema=req.schema_filter)
    changes, risk = compute_diff(actual, desired)

    from collections import Counter
    from datetime import datetime, timezone
    from app.models.schemas import PlanMetadata

    rollback_parts = [c.sql_down for c in changes if c.sql_down]
    rollback_sql = "\n".join(reversed(rollback_parts))

    metadata = PlanMetadata(
        created_at=datetime.now(timezone.utc).isoformat(),
        dictionary_path="<api>",
        dictionary_hash="",
        connection=_sanitize_conn(req.connection_string),
        engine=req.engine,
        schema_filter=req.schema_filter,
    )

    return PlanResult(
        metadata=metadata,
        changes=changes,
        risk_level=risk,
        plan_hash=_changes_hash(changes),
        rollback_sql=rollback_sql,
        summary=dict(Counter(c.change_type.value for c in changes)),
    )


# --- Apply ---

@router.post("/apply", response_model=ApplyResponse)
def apply(req: ApplyRequest):
    """Apply a plan to a live database."""
    applied, rollback_sql = apply_plan(
        req.connection_string, req.plan, dry_run=req.dry_run
    )
    return ApplyResponse(applied=applied, rollback_sql=rollback_sql, dry_run=req.dry_run)


# --- Drift (legacy — baseline) ---

@router.post("/drift/check", response_model=DriftCheckResponse)
def drift_check(req: DriftCheckRequest):
    actual = introspect_schema(req.connection_string)
    baseline: dict = {"tables": {}}
    diffs, _ = compute_diff(actual, baseline)
    return DriftCheckResponse(drifted=len(diffs) > 0, diffs=diffs)


# --- Drift (dictionary mode) ---

@router.post("/drift/dictionary", response_model=DriftReport)
def drift_dictionary(req: DriftDictionaryRequest):
    """Detect drift: compare live DB against data dictionary."""
    dd = DataDictionary.model_validate(req.dictionary)
    desired = dictionary_to_desired(dd, engine=req.engine, schema_filter=req.schema_filter)
    actual = introspect_schema(req.connection_string, schema=req.schema_filter)
    return detect_drift_from_schemas(
        actual, desired, dictionary=dd,
        profile=req.profile, schema_filter=req.schema_filter,
    )


# --- Compile ---

@router.post("/compile", response_model=CompileResponse)
def compile(req: CompileRequest):
    """Compile dictionary into DDL for the specified engine."""
    dd = DataDictionary.model_validate(req.dictionary)

    if req.engine == "redis":
        from app.core.redis_compile import compile_redis
        if not dd.redis_patterns:
            return CompileResponse(engine="redis", profile=req.profile, entity_count=0, files=[])
        result = compile_redis(dd.redis_patterns)
        return CompileResponse(
            engine="redis",
            profile=req.profile,
            entity_count=len(result.patterns),
            files=[
                {"name": "redis-config.json", "content": json.dumps(result.app_config, indent=2)},
                {"name": "redis-setup.sh", "content": result.cli_script},
                {"name": "redis-patterns.md", "content": result.docs},
            ],
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        compile_result = compile_and_write(
            dd, engine=req.engine, profile=req.profile,
            output_dir=tmpdir, schema_filter=req.schema_filter,
        )
    return CompileResponse(
        engine=compile_result.engine,
        profile=compile_result.profile,
        entity_count=compile_result.entity_count,
        files=compile_result.files,
        index_count=compile_result.index_count,
        fk_count=compile_result.fk_count,
        check_count=compile_result.check_count,
        comment_count=compile_result.comment_count,
    )


# --- Validate ---

@router.post("/validate", response_model=ValidateResponse)
def validate(req: ValidateRequest):
    """Run structural and security checks on the data dictionary."""
    dd = DataDictionary.model_validate(req.dictionary)
    checks = get_suite_checks(req.suite) if req.suite else None
    report = validate_dictionary(dd, checks=checks, schema_filter=req.schema_filter)
    return ValidateResponse(
        total_checks=report.total_checks,
        passed=report.passed,
        failed=report.failed,
        warnings=report.warnings,
        all_passed=report.all_passed,
        results=[r.model_dump() for r in report.results if not r.passed],
    )


# --- Maetel ---

@router.post("/schema/maetel", response_model=MaetelResponse)
def schema_maetel(req: MaetelRequest):
    """Generate ER diagram from a live database."""
    schema = introspect_schema(req.connection_string, schema=req.schema_name)
    if req.format == "json":
        result = maetel_to_json(schema, schema_name=req.schema_name)
        content = json.dumps(result, indent=2, ensure_ascii=False)
        stats = result.get("stats", {})
    else:
        content = maetel_to_mermaid(schema, schema_name=req.schema_name)
        stats = {"line_count": len(content.splitlines())}
    return MaetelResponse(format=req.format, content=content, stats=stats)


# --- Health ---

@router.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(version=VERSION)
