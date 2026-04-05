"""hale-bopp-db CLI — schema governance from the terminal.

Usage:
    hb diff --connection <conn> --desired schema.json
    hb deploy --connection <conn> --changes changes.json [--execute]
    hb drift --connection <conn> --baseline baseline.json
    hb snapshot --connection <conn> -o baseline.json
    hb plan --connection <conn> --dictionary dict.json [-o plan.json]
    hb apply --connection <conn> --plan plan.json [--execute]
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import click

from app.version import __version__
from app.core.compile import compile_and_write, load_dictionary
from app.core.docs import generate_dbml, generate_excel, generate_html, generate_markdown, generate_mermaid
from app.core.validate import ValidationReport, get_suite_checks, validate_dictionary
from app.core.deploy import deploy_changes
from app.core.diff import compute_diff
from app.core.maetel import to_json as maetel_to_json
from app.core.maetel import to_mermaid as maetel_to_mermaid
from app.core.introspect import introspect_schema
from app.core.drift_detect import detect_drift
from app.core.plan import apply_plan, create_plan, load_plan, save_plan
from app.core.profiles import resolve_env
from app.models.schemas import DriftItem, SchemaChange


def _sanitize_conn(conn: str) -> str:
    """Hide password in connection string for display."""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", conn)


def _resolve_from_env(env_name: str | None, connection: str | None, engine: str | None, profile: str | None) -> tuple[str, str, str]:
    """Resolve connection/engine/profile from --env or explicit flags.

    Returns (connection, engine, profile). Raises click.UsageError on conflicts.
    """
    if env_name:
        try:
            env = resolve_env(env_name)
        except (FileNotFoundError, ValueError) as exc:
            raise click.UsageError(str(exc))
        conn = connection or env.connection
        eng = engine or env.engine
        prof = profile or env.profile
        return conn, eng, prof

    if not connection:
        raise click.UsageError("Provide --connection (-c) or --env.")
    return connection, engine or "pg", profile or "essential"


def _print_drift_items(items: list[DriftItem]) -> None:
    """Pretty-print drift findings to stdout."""
    type_icons = {
        "extra_table": ("+", "yellow"),
        "missing_table": ("-", "red"),
        "extra_column": ("+", "yellow"),
        "missing_column": ("-", "red"),
        "type_mismatch": ("~", "magenta"),
        "missing_index": ("-", "red"),
        "extra_index": ("+", "yellow"),
        "missing_rls": ("!", "red"),
        "missing_masking": ("!", "red"),
    }
    for item in items:
        dt = item.drift_type.value
        icon, color = type_icons.get(dt, ("?", "white"))
        click.secho(f"  {icon} [{dt}] {item.object_name}", fg=color)
        if item.suggested_action:
            click.echo(f"    FIX: {item.suggested_action}")


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
@click.version_option(__version__, prog_name="hb")
def cli():
    """hale-bopp-db — Deterministic schema governance for PostgreSQL."""
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
@click.option("--connection", "-c", default=None, help="PostgreSQL connection string")
@click.option("--baseline", "-b", type=click.Path(exists=True), default=None, help="Baseline schema JSON file (legacy mode)")
@click.option("--dictionary", "-d", type=click.Path(exists=True), default=None, help="Data dictionary JSON file (recommended)")
@click.option("--engine", "-e", type=click.Choice(["pg", "mssql", "oracle"]), default=None, help="Target engine")
@click.option("--profile", "-p", type=click.Choice(["essential", "standard", "enterprise"]), default=None, help="Security profile for RLS/masking checks")
@click.option("--schema", "-s", "schema_filter", default=None, help="Filter by schema")
@click.option("--env", "env_name", default=None, help="Environment from hb-profiles.yml")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def drift(connection: str | None, baseline: str | None, dictionary: str | None, engine: str | None, profile: str | None, schema_filter: str | None, env_name: str | None, json_output: bool):
    """Detect schema drift against the data dictionary or a saved baseline.

    Dictionary mode (recommended): compares DB against db-data-dictionary.json,
    including security drift (RLS, masking) based on the security profile.

    Baseline mode (legacy): compares DB against a snapshot JSON file.
    Use --env to load connection/engine/profile from hb-profiles.yml.
    """
    if not baseline and not dictionary:
        click.secho("ERROR: provide --dictionary (-d) or --baseline (-b)", fg="red", err=True)
        sys.exit(2)

    conn, eng, prof = _resolve_from_env(env_name, connection, engine, profile)

    if not json_output:
        click.echo(f"Connecting to {_sanitize_conn(conn)}...")

    # --- Dictionary mode (PBI #548) ---
    if dictionary:
        report = detect_drift(
            connection_string=conn,
            dictionary_path=dictionary,
            engine=eng,
            profile=prof,
            schema_filter=schema_filter,
        )

        if json_output:
            click.echo(report.model_dump_json(indent=2))
        else:
            if report.has_drift:
                click.secho(f"DRIFT DETECTED — {len(report.items)} finding(s)", fg="red", bold=True)
                click.echo(f"Profile: {prof} | Engine: {eng}")
                if schema_filter:
                    click.echo(f"Schema filter: {schema_filter}")
                click.echo()
                _print_drift_items(report.items)
                click.echo()
                click.echo("Summary:")
                for dtype, count in sorted(report.summary.items()):
                    click.echo(f"  {dtype}: {count}")
            else:
                click.secho("No drift detected. Database matches dictionary.", fg="green")

        sys.exit(1 if report.has_drift else 0)

    # --- Baseline mode (legacy) ---
    with open(baseline, encoding="utf-8") as f:
        baseline_schema = json.load(f)

    actual = introspect_schema(conn)
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


@cli.command()
@click.option("--engine", "-e", type=click.Choice(["pg", "mssql", "oracle", "redis"]), default=None, help="Target engine (pg, redis)")
@click.option("--profile", "-p", type=click.Choice(["essential", "standard", "enterprise"]), default=None, help="Security profile")
@click.option("--dictionary", "-d", required=True, type=click.Path(exists=True), help="Data dictionary JSON file")
@click.option("--output", "-o", required=True, type=click.Path(), help="Output directory for SQL/config files")
@click.option("--schema", "-s", "schema_filter", default=None, help="Filter by schema (e.g. platform)")
@click.option("--env", "env_name", default=None, help="Environment from hb-profiles.yml (e.g. dev, staging, prod)")
@click.option("--json-output", "-j", is_flag=True, help="Output summary as JSON")
def compile(engine: str | None, profile: str | None, dictionary: str, output: str, schema_filter: str | None, env_name: str | None, json_output: bool):
    """Compile data dictionary into DDL (pg) or config (redis).

    Reads db-data-dictionary.json and generates idempotent output files.
    Use --env to load engine/profile from hb-profiles.yml.
    """
    # Resolve from env or defaults
    if env_name:
        _, eng, prof = _resolve_from_env(env_name, None, engine, profile)
    else:
        eng = engine or "pg"
        prof = profile or "essential"

    click.echo(f"Loading dictionary: {dictionary}")
    dd = load_dictionary(dictionary)

    # --- Redis engine ---
    if eng == "redis":
        from app.core.redis_compile import compile_redis_and_write
        if not dd.redis_patterns:
            click.secho("No redis_patterns found in dictionary.", fg="yellow")
            sys.exit(0)

        click.echo(f"Compiling {len(dd.redis_patterns)} Redis patterns")
        result = compile_redis_and_write(dd.redis_patterns, output_dir=output)

        if json_output:
            click.echo(json.dumps(result.app_config, indent=2))
        else:
            click.secho(f"\nRedis compile complete:", fg="green", bold=True)
            click.echo(f"  Patterns:    {len(result.patterns)}")
            click.echo(f"  Files:")
            click.echo(f"    → {output}/redis-setup.sh")
            click.echo(f"    → {output}/redis-config.json")
            click.echo(f"    → {output}/redis-patterns.md")
        return

    # --- PG engine ---
    click.echo(f"Compiling {len(dd.entities)} entities → {eng.upper()} (profile: {prof})")
    result = compile_and_write(dd, engine=eng, profile=prof, output_dir=output, schema_filter=schema_filter)

    if json_output:
        summary = {
            "engine": result.engine,
            "profile": result.profile,
            "entities": result.entity_count,
            "files": [f["name"] for f in result.files],
            "indexes": result.index_count,
            "foreign_keys": result.fk_count,
            "check_constraints": result.check_count,
            "comments": result.comment_count,
            "output_dir": output,
        }
        click.echo(json.dumps(summary, indent=2))
    else:
        click.secho(f"\nCompile complete:", fg="green", bold=True)
        click.echo(f"  Engine:      {result.engine.upper()}")
        click.echo(f"  Profile:     {result.profile}")
        click.echo(f"  Entities:    {result.entity_count}")
        click.echo(f"  Files:       {len(result.files)}")
        for f in result.files:
            click.echo(f"    → {output}/{f['name']}")
        click.echo(f"  Indexes:     {result.index_count}")
        click.echo(f"  FK:          {result.fk_count}")
        click.echo(f"  CHECK:       {result.check_count}")
        click.echo(f"  COMMENT ON:  {result.comment_count}")


@cli.command("test")
@click.option("--dictionary", "-d", required=True, type=click.Path(exists=True), help="Data dictionary JSON file")
@click.option("--suite", type=click.Choice(["naming", "types", "structure", "security"]), default=None, help="Run only a specific check suite")
@click.option("--schema", "-s", "schema_filter", default=None, help="Filter by schema (e.g. platform)")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def test_cmd(dictionary: str, suite: str | None, schema_filter: str | None, json_output: bool):
    """Run structural and security checks on the data dictionary.

    7 checks: canonical names, valid types, no duplicates, PII, naming
    convention, FK validity, standard columns.
    """
    dd = load_dictionary(dictionary)

    checks = get_suite_checks(suite) if suite else None
    report = validate_dictionary(dd, checks=checks, schema_filter=schema_filter)

    if json_output:
        output = {
            "total_checks": report.total_checks,
            "passed": report.passed,
            "failed": report.failed,
            "warnings": report.warnings,
            "all_passed": report.all_passed,
            "results": [r.model_dump() for r in report.results if not r.passed],
        }
        click.echo(json.dumps(output, indent=2))
    else:
        for r in report.results:
            if r.passed:
                icon = click.style("OK", fg="green")
            elif r.severity.value == "warning":
                icon = click.style("WARN", fg="yellow")
            else:
                icon = click.style("FAIL", fg="red")

            target = r.entity
            if r.column:
                target = f"{r.entity}.{r.column}"

            if r.passed:
                click.echo(f"  {icon}  {r.check_name:<20s} {target}")
            else:
                click.echo(f"  {icon}  {r.check_name:<20s} {target}: {r.message}")
                if r.fix:
                    fix_label = "AUTO-FIX" if r.auto_fix else "FIX"
                    click.echo(f"        {fix_label}: {r.fix}")

        click.echo()
        if report.all_passed:
            click.secho(f"All checks passed. {report.passed}/{report.total_checks}", fg="green", bold=True)
        else:
            color = "red" if report.failed else "yellow"
            click.secho(
                f"{report.failed} error(s), {report.warnings} warning(s) "
                f"out of {report.total_checks} checks.",
                fg=color, bold=True,
            )

    sys.exit(0 if report.all_passed else 1)


@cli.command()
@click.option("--connection", "-c", default=None, help="PostgreSQL connection string")
@click.option("--dictionary", "-d", required=True, type=click.Path(exists=True), help="Data dictionary JSON file")
@click.option("--output", "-o", type=click.Path(), default=None, help="Output plan file (default: halebopp-plan.json)")
@click.option("--engine", "-e", type=click.Choice(["pg", "mssql", "oracle"]), default=None, help="Target engine")
@click.option("--schema", "-s", "schema_filter", default=None, help="Filter by schema")
@click.option("--env", "env_name", default=None, help="Environment from hb-profiles.yml")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON to stdout")
def plan(connection: str | None, dictionary: str, output: str | None, engine: str | None, schema_filter: str | None, env_name: str | None, json_output: bool):
    """Compare dictionary (desired) vs live DB (actual) and produce a plan.

    The plan shows what CREATE, ALTER, DROP statements would be needed
    to bring the database in sync with the data dictionary.  No changes
    are applied — use 'halebopp apply' to execute.

    Use --env to load connection/engine from hb-profiles.yml.
    """
    conn, eng, _ = _resolve_from_env(env_name, connection, engine, None)

    if not json_output:
        click.echo(f"Loading dictionary: {dictionary}")
        click.echo(f"Connecting to {_sanitize_conn(conn)}...")

    result = create_plan(
        connection_string=conn,
        dictionary_path=dictionary,
        engine=eng,
        schema_filter=schema_filter,
    )

    # Save plan file
    plan_path = output or "halebopp-plan.json"
    save_plan(result, plan_path)

    if json_output:
        click.echo(result.model_dump_json(indent=2))
    else:
        if not result.changes:
            click.secho("No changes needed — database matches dictionary.", fg="green")
        else:
            _print_changes(result.changes, result.risk_level.value)
            click.echo()
            click.secho(f"Plan saved: {plan_path}", fg="cyan", bold=True)
            click.echo(f"  Dictionary hash: {result.metadata.dictionary_hash[:16]}...")
            click.echo(f"  Plan hash:       {result.plan_hash[:16]}...")
            click.echo(f"\nReview the plan, then run:")
            click.secho(f"  halebopp apply -c <conn> --plan {plan_path} --execute", fg="yellow")

    sys.exit(1 if result.changes else 0)


@cli.command("apply")
@click.option("--connection", "-c", default=None, help="PostgreSQL connection string")
@click.option("--plan", "plan_file", required=True, type=click.Path(exists=True), help="Plan file from 'halebopp plan'")
@click.option("--execute", is_flag=True, help="Actually apply changes (default is dry-run)")
@click.option("--env", "env_name", default=None, help="Environment from hb-profiles.yml")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def apply_cmd(connection: str | None, plan_file: str, execute: bool, env_name: str | None, json_output: bool):
    """Apply a plan to a live database.

    Reads a plan file produced by 'halebopp plan', verifies its integrity
    (SHA-256 hash), and applies the changes inside a single transaction.
    Rolls back automatically on any failure.

    Default mode is dry-run — use --execute to apply for real.
    Use --env to load connection from hb-profiles.yml.
    """
    conn, _, _ = _resolve_from_env(env_name, connection, None, None)

    if not json_output:
        click.echo(f"Loading plan: {plan_file}")

    try:
        loaded_plan = load_plan(plan_file)
    except ValueError as exc:
        click.secho(f"ERROR: {exc}", fg="red", err=True)
        sys.exit(2)

    if not loaded_plan.changes:
        click.secho("Plan has no changes — nothing to apply.", fg="green")
        sys.exit(0)

    if not execute and not json_output:
        click.secho("DRY RUN — no changes applied. Use --execute to apply.", fg="yellow")

    if not json_output:
        click.echo(f"Connecting to {_sanitize_conn(conn)}...")
        click.echo(f"Risk level: {loaded_plan.risk_level.value.upper()}")
        click.echo(f"Changes: {len(loaded_plan.changes)}")

    applied, rollback_sql = apply_plan(conn, loaded_plan, dry_run=not execute)

    if json_output:
        out = {
            "applied": [c.model_dump() for c in applied],
            "rollback_sql": rollback_sql,
            "dry_run": not execute,
            "plan_hash": loaded_plan.plan_hash,
        }
        click.echo(json.dumps(out, indent=2))
    else:
        action = "Applied" if execute else "Would apply"
        click.secho(f"\n{action} {len(applied)} change(s).", fg="green" if execute else "yellow", bold=True)
        if rollback_sql and not execute:
            click.echo(f"\nRollback SQL available in plan file.")


@cli.group()
def docs():
    """Generate documentation from the data dictionary."""
    pass


@docs.command("generate")
@click.option("--dictionary", "-d", required=True, type=click.Path(exists=True), help="Data dictionary JSON file")
@click.option("--output", "-o", required=True, type=click.Path(), help="Output directory")
@click.option("--schema", "-s", "schema_filter", default=None, help="Filter by schema")
def docs_generate(dictionary: str, output: str, schema_filter: str | None):
    """Generate full documentation: HTML, Markdown, Mermaid, DBML."""
    dd = load_dictionary(dictionary)
    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)

    # HTML
    html = generate_html(dd, schema_filter=schema_filter)
    (out / "index.html").write_text(html, encoding="utf-8")

    # Markdown
    md = generate_markdown(dd, schema_filter=schema_filter)
    (out / "data-dictionary.md").write_text(md, encoding="utf-8")

    # Mermaid
    mermaid = generate_mermaid(dd, schema_filter=schema_filter)
    (out / "er-diagram.mmd").write_text(mermaid, encoding="utf-8")

    # DBML
    dbml = generate_dbml(dd, schema_filter=schema_filter)
    (out / "schema.dbml").write_text(dbml, encoding="utf-8")

    click.secho("Docs generated:", fg="green", bold=True)
    click.echo(f"  → {out}/index.html")
    click.echo(f"  → {out}/data-dictionary.md")
    click.echo(f"  → {out}/er-diagram.mmd")
    click.echo(f"  → {out}/schema.dbml")


@docs.command("export")
@click.option("--dictionary", "-d", required=True, type=click.Path(exists=True), help="Data dictionary JSON file")
@click.option("--format", "-f", "fmt", required=True, type=click.Choice(["excel", "mermaid", "dbml", "markdown", "html"]), help="Export format")
@click.option("--output", "-o", required=True, type=click.Path(), help="Output file path")
@click.option("--schema", "-s", "schema_filter", default=None, help="Filter by schema")
def docs_export(dictionary: str, fmt: str, output: str, schema_filter: str | None):
    """Export documentation in a specific format."""
    dd = load_dictionary(dictionary)

    if fmt == "excel":
        path = generate_excel(dd, output_path=output)
        click.secho(f"Excel exported: {path}", fg="green")
    elif fmt == "mermaid":
        content = generate_mermaid(dd, schema_filter=schema_filter)
        Path(output).write_text(content, encoding="utf-8")
        click.secho(f"Mermaid exported: {output}", fg="green")
    elif fmt == "dbml":
        content = generate_dbml(dd, schema_filter=schema_filter)
        Path(output).write_text(content, encoding="utf-8")
        click.secho(f"DBML exported: {output}", fg="green")
    elif fmt == "markdown":
        content = generate_markdown(dd, schema_filter=schema_filter)
        Path(output).write_text(content, encoding="utf-8")
        click.secho(f"Markdown exported: {output}", fg="green")
    elif fmt == "html":
        content = generate_html(dd, schema_filter=schema_filter)
        Path(output).write_text(content, encoding="utf-8")
        click.secho(f"HTML exported: {output}", fg="green")


def _count_tables(schema: dict, schema_name: str | None) -> int:
    """Count tables in the schema."""
    if schema_name and "schemas" in schema:
        data = schema["schemas"].get(schema_name, {})
        return len(data.get("tables", {}))
    return len(schema.get("tables", {}))


if __name__ == "__main__":
    cli()
