"""Schema deploy engine — applies changes transactionally with audit logging."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

from app.models.schemas import SchemaChange

AUDIT_LOG_PATH = os.environ.get("HALEBOPP_AUDIT_LOG", "halebopp-audit.jsonl")


def _sanitize_conn(conn: str) -> str:
    """Remove password from connection string for audit."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", conn)


def _write_audit(entry: dict) -> None:
    """Append an entry to the JSONL audit log."""
    path = Path(AUDIT_LOG_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def deploy_changes(
    connection_string: str,
    changes: list[SchemaChange],
    dry_run: bool = True,
) -> tuple[list[SchemaChange], str]:
    """Apply schema changes inside a transaction. Returns applied changes and rollback SQL."""
    rollback_statements = [c.sql_down for c in changes if c.sql_down]
    rollback_sql = "\n".join(reversed(rollback_statements))

    audit_entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "connection": _sanitize_conn(connection_string),
        "dry_run": dry_run,
        "changes_count": len(changes),
        "status": "pending",
    }

    if dry_run:
        audit_entry["status"] = "dry_run"
        _write_audit(audit_entry)
        return changes, rollback_sql

    engine = create_engine(connection_string)
    applied: list[SchemaChange] = []

    try:
        with engine.begin() as conn:
            for change in changes:
                if not change.sql_up:
                    continue
                conn.execute(text(change.sql_up))
                applied.append(change)

        audit_entry["status"] = "success"
        audit_entry["applied_count"] = len(applied)
    except Exception as exc:
        audit_entry["status"] = "failed"
        audit_entry["error"] = str(exc)
        _write_audit(audit_entry)
        raise
    finally:
        engine.dispose()

    _write_audit(audit_entry)
    return applied, rollback_sql
