"""DB-HALE-BOPP REST API routes."""

from __future__ import annotations

from fastapi import APIRouter

from hale_bopp_db.core.deploy import deploy_changes
from hale_bopp_db.core.diff import compute_diff
from hale_bopp_db.core.introspect import introspect_schema
from hale_bopp_db.models.schemas import (
    DeployRequest,
    DeployResponse,
    DiffRequest,
    DiffResponse,
    DriftCheckRequest,
    DriftCheckResponse,
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


@router.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(version=VERSION)
