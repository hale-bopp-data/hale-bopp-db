"""DB-HALE-BOPP CLI — schema governance from the terminal.

Usage:
    halebopp diff --connection <conn> --desired schema.json
    halebopp deploy --connection <conn> --changes changes.json [--execute]
    halebopp drift --connection <conn> --baseline baseline.json
    halebopp snapshot --connection <conn> -o baseline.json
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import click

from app.core.deploy import deploy_changes
from app.core.diff import compute_diff
from app.core.maetel import to_json as maetel_to_json
from app.core.maetel import to_mermaid as maetel_to_mermaid
from app.core.introspect import introspect_schema
from app.models.schemas import SchemaChange


def _sanitize_conn(conn: str) -> str:
    """Hide password in connection string for display."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", conn)


def _print_changes(changes: list[SchemaChange], risk: str) -> None:
    """Pretty-print schema changes to stdout."""
    if not changes:
        click.secho("No changes detected.", fg="green")
        return

    risk_colors = {"low": "green", "medium": "yellow", "high": "red"}
    click.secho(f"Risk: {risk.upper()}", fg=risk_colors.get(risk, "white"), bold=True)
    click.echo(f"Changes: {len(changes)}\n")

    for c in changes:
        ct = c.change_type.value if hasattr(c.change_type, "value") else c.change_type
        icon = {"add_table": "+", "drop_table": "-", "add_column": "+",
                "drop_column": "-", "alter_column": "~"}.get(ct, "?")
        color = {"add_table": "green", "drop_table": "red", "add_column": "green",
                 "drop_column": "red", "alter_column": "yellow"}.get(ct, "white")
        click.secho(f"  {icon} [{ct}] {c.object_name}", fg=color)
        if c.sql_up:
            click.echo(f"    SQL: {c.sql_up.strip()}")


@click.group()
@click.version_option("0.1.0", prog_name="halebopp")
def cli():
    """DB-HALE-BOPP — Deterministic schema governance for PostgreSQL."""
    pass


@cli.command()
@click.option("--connection", "-c", required=True, help="PostgreSQL connection string")
@click.option("--desired", "-d", required=True, type=click.Path(exists=True), help="Desired schema JSON file")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def diff(connection: str, desired: str, json_output: bool):
    """Compare actual schema with desired schema."""
    if not json_output:
        click.echo(f"Connecting to {_sanitize_conn(connection)}...")

    with open(desired, encoding="utf-8") as f:
        desired_schema = json.load(f)

    actual = introspect_schema(connection)
    changes, risk = compute_diff(actual, desired_schema)

    if json_output:
        output = {
            "changes": [c.model_dump() for c in changes],
            "risk_level": risk.value,
        }
        click.echo(json.dumps(output, indent=2))
    else:
        _print_changes(changes, risk.value)

    sys.exit(1 if changes else 0)


@cli.command()
@click.option("--connection", "-c", required=True, help="PostgreSQL connection string")
@click.option("--changes", required=True, type=click.Path(exists=True), help="Changes JSON file (from diff --json-output)")
@click.option("--execute", is_flag=True, help="Actually apply changes (default is dry-run)")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def deploy(connection: str, changes: str, execute: bool, json_output: bool):
    """Apply schema changes transactionally."""
    with open(changes, encoding="utf-8") as f:
        raw = json.load(f)

    change_list = [SchemaChange(**c) for c in raw.get("changes", raw if isinstance(raw, list) else [])]

    if not execute:
        click.secho("DRY RUN — no changes applied. Use --execute to apply.", fg="yellow")

    click.echo(f"Connecting to {_sanitize_conn(connection)}...")
    applied, rollback_sql = deploy_changes(connection, change_list, dry_run=not execute)

    if json_output:
        output = {
            "applied": [c.model_dump() for c in applied],
            "rollback_sql": rollback_sql,
            "dry_run": not execute,
        }
        click.echo(json.dumps(output, indent=2))
    else:
        action = "Applied" if execute else "Would apply"
        click.echo(f"{action} {len(applied)} change(s).")
        if rollback_sql:
            click.echo(f"\nRollback SQL:\n{rollback_sql}")


@cli.command()
@click.option("--connection", "-c", required=True, help="PostgreSQL connection string")
@click.option("--baseline", "-b", required=True, type=click.Path(exists=True), help="Baseline schema JSON file")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def drift(connection: str, baseline: str, json_output: bool):
    """Detect schema drift against a saved baseline."""
    click.echo(f"Connecting to {_sanitize_conn(connection)}...")

    with open(baseline, encoding="utf-8") as f:
        baseline_schema = json.load(f)

    actual = introspect_schema(connection)
    # Drift = diff between baseline (desired) and actual
    # If actual has things not in baseline, that's drift
    changes, risk = compute_diff(baseline_schema, actual)

    if json_output:
        output = {
            "drifted": len(changes) > 0,
            "diffs": [c.model_dump() for c in changes],
            "risk_level": risk.value,
        }
        click.echo(json.dumps(output, indent=2))
    else:
        if changes:
            click.secho(f"DRIFT DETECTED — {len(changes)} difference(s)", fg="red", bold=True)
            _print_changes(changes, risk.value)
        else:
            click.secho("No drift detected. Schema matches baseline.", fg="green")

    sys.exit(1 if changes else 0)


@cli.command()
@click.option("--connection", "-c", required=True, help="PostgreSQL connection string")
@click.option("--output", "-o", required=True, type=click.Path(), help="Output JSON file path")
def snapshot(connection: str, output: str):
    """Save current schema as a JSON baseline."""
    click.echo(f"Connecting to {_sanitize_conn(connection)}...")

    schema = introspect_schema(connection)

    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, sort_keys=True)

    table_count = len(schema.get("tables", {}))
    click.secho(f"Snapshot saved: {out_path} ({table_count} tables)", fg="green")


@cli.command()
@click.option("--connection", "-c", required=True, help="PostgreSQL connection string")
@click.option("--format", "-f", "fmt", type=click.Choice(["mermaid", "json"]), default="mermaid", help="Output format")
@click.option("--schema", "-s", "schema_name", default=None, help="Filter by schema (e.g. platform)")
@click.option("--output", "-o", "output_file", type=click.Path(), default=None, help="Output file path (stdout if omitted)")
def maetel(connection: str, fmt: str, schema_name: str | None, output_file: str | None):
    """Generate ER diagram from a live database.

    Named after Maetel from Galaxy Express 999 — the guide who knows every stop.
    """
    click.echo(f"Connecting to {_sanitize_conn(connection)}...")

    schema = introspect_schema(connection, schema=schema_name)

    if fmt == "mermaid":
        content = maetel_to_mermaid(schema, schema_name=schema_name)
        if output_file:
            # Wrap in markdown code block for .md files
            if output_file.endswith(".md"):
                content = f"```mermaid\n{content}\n```"
    else:
        result = maetel_to_json(schema, schema_name=schema_name)
        content = json.dumps(result, indent=2, ensure_ascii=False)

    if output_file:
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(content)

        table_count = _count_tables(schema, schema_name)
        click.secho(f"Maetel saved: {out_path} ({table_count} tables)", fg="green")
    else:
        click.echo(content)


def _count_tables(schema: dict, schema_name: str | None) -> int:
    """Count tables in the schema."""
    if schema_name and "schemas" in schema:
        data = schema["schemas"].get(schema_name, {})
        return len(data.get("tables", {}))
    return len(schema.get("tables", {}))


if __name__ == "__main__":
    cli()
