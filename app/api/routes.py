"""DB-HALE-BOPP REST API routes."""

from __future__ import annotations

from fastapi import APIRouter

from app.core.deploy import deploy_changes
from app.core.diff import compute_diff
from app.core.maetel import to_json as maetel_to_json
from app.core.maetel import to_mermaid as maetel_to_mermaid
from app.core.introspect import introspect_schema
from app.models.schemas import (
    DeployRequest,
    DeployResponse,
    DiffRequest,
    DiffResponse,
    DriftCheckRequest,
    DriftCheckResponse,
    MaetelRequest,
    MaetelResponse,
    HealthResponse,
)

router = APIRouter(prefix="/api/v1")

VERSION = "0.1.0"


@router.post("/schema/diff", response_model=DiffResponse)
def schema_diff(req: DiffRequest):
    actual = introspect_schema(req.connection_string)
    changes, risk = compute_diff(actual, req.desired_schema)
    return DiffResponse(changes=changes, risk_level=risk)


@router.post("/schema/deploy", response_model=DeployResponse)
def schema_deploy(req: DeployRequest):
    applied, rollback_sql = deploy_changes(
        req.connection_string, req.changes, req.dry_run
    )
    return DeployResponse(applied=applied, rollback_sql=rollback_sql)


@router.post("/drift/check", response_model=DriftCheckResponse)
def drift_check(req: DriftCheckRequest):
    actual = introspect_schema(req.connection_string)
    # For MVP, compare against empty baseline if no baseline_id provided.
    # Future: load baseline from metadata registry.
    baseline: dict = {"tables": {}}
    diffs, _ = compute_diff(actual, baseline)
    # Invert: if actual has things not in baseline, that's drift
    # For now, any diff = drift
    return DriftCheckResponse(drifted=len(diffs) > 0, diffs=diffs)


@router.post("/schema/maetel", response_model=MaetelResponse)
def schema_maetel(req: MaetelRequest):
    """Generate ER diagram from a live database.

    Named after Maetel from Galaxy Express 999 — the guide who knows every stop.
    """
    schema = introspect_schema(req.connection_string, schema=req.schema_name)
    if req.format == "json":
        result = maetel_to_json(schema, schema_name=req.schema_name)
        import json
        content = json.dumps(result, indent=2, ensure_ascii=False)
        stats = result.get("stats", {})
    else:
        content = maetel_to_mermaid(schema, schema_name=req.schema_name)
        stats = {"line_count": len(content.splitlines())}
    return MaetelResponse(format=req.format, content=content, stats=stats)


@router.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(version=VERSION)
