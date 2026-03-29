"""hb test — 7 structural and security checks for the Data Dictionary.

Like dbt tests data, we test structure. Every entity and column must pass
all 7 checks before compile.

Checks:
  1. canonical_name    — name is in name_mapping? no forbidden aliases?
  2. valid_type        — logical type exists in type_map?
  3. no_duplicate      — no duplicate column names within an entity?
  4. pii_check         — PII columns marked? masking configured?
  5. naming_convention — snake_case? no abbreviations?
  6. fk_valid          — FK target entity exists?
  7. standard_columns  — created_at, created_by, updated_at, updated_by present?

Phase 1: PBI #545.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from app.core.compile import DataDictionary, EntityDef


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class CheckSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class CheckResult(BaseModel):
    check_id: int
    check_name: str
    entity: str
    column: str | None = None
    passed: bool
    severity: CheckSeverity = CheckSeverity.ERROR
    message: str = ""
    fix: str | None = None
    auto_fix: bool = False


class ValidationReport(BaseModel):
    total_checks: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    results: list[CheckResult] = Field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return self.failed == 0


# ---------------------------------------------------------------------------
# Suites
# ---------------------------------------------------------------------------

SUITES: dict[str, list[int]] = {
    "naming": [1, 5],
    "types": [2],
    "structure": [3, 6, 7],
    "security": [4],
}

ALL_CHECKS = [1, 2, 3, 4, 5, 6, 7]


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_canonical_name(
    entity: EntityDef,
    name_mapping: list[dict[str, Any]],
) -> list[CheckResult]:
    """Check 1: entity and column names are canonical, no forbidden aliases."""
    results: list[CheckResult] = []

    # Build forbidden alias → canonical lookup
    forbidden_to_canonical: dict[str, str] = {}
    for mapping in name_mapping:
        canonical = mapping.get("canonical", "")
        for alias in mapping.get("aliases_forbidden", []):
            forbidden_to_canonical[alias.lower()] = canonical

    # Check entity name
    if entity.name.lower() in forbidden_to_canonical:
        canonical = forbidden_to_canonical[entity.name.lower()]
        results.append(CheckResult(
            check_id=1, check_name="canonical_name", entity=entity.name,
            passed=False, message=f"Entity name '{entity.name}' is a forbidden alias",
            fix=f"Rename to '{canonical}'", auto_fix=True,
        ))

    # Check column names
    for col in entity.columns:
        if col.name.lower() in forbidden_to_canonical:
            canonical = forbidden_to_canonical[col.name.lower()]
            results.append(CheckResult(
                check_id=1, check_name="canonical_name", entity=entity.name,
                column=col.name, passed=False,
                message=f"Column '{col.name}' is a forbidden alias",
                fix=f"Rename to '{canonical}'", auto_fix=True,
            ))

    if not results:
        results.append(CheckResult(
            check_id=1, check_name="canonical_name", entity=entity.name,
            passed=True, message="All names are canonical",
        ))

    return results


def _check_valid_type(
    entity: EntityDef,
    type_map: dict[str, dict[str, str]],
) -> list[CheckResult]:
    """Check 2: all logical types exist in the type_map."""
    results: list[CheckResult] = []

    # Build set of valid base types (including parameterised patterns)
    valid_bases: set[str] = set()
    parameterised_bases: set[str] = set()
    for key in type_map:
        m = re.match(r"^(\w+)\(", key)
        if m:
            parameterised_bases.add(m.group(1))
        else:
            valid_bases.add(key)

    has_failure = False
    for col in entity.columns:
        logical = col.type
        # Direct match
        if logical in valid_bases:
            continue
        # Parameterised match: string(50) → base "string" in parameterised_bases
        m = re.match(r"^(\w+)\(", logical)
        if m and m.group(1) in parameterised_bases:
            continue
        # Invalid
        has_failure = True
        results.append(CheckResult(
            check_id=2, check_name="valid_type", entity=entity.name,
            column=col.name, passed=False,
            message=f"Type '{logical}' not found in type_map",
            fix=f"Valid types: {', '.join(sorted(valid_bases | {b + '(...)' for b in parameterised_bases}))}",
        ))

    if not has_failure:
        results.append(CheckResult(
            check_id=2, check_name="valid_type", entity=entity.name,
            passed=True, message="All types are valid",
        ))

    return results


def _check_no_duplicate(entity: EntityDef) -> list[CheckResult]:
    """Check 3: no duplicate column names within an entity."""
    seen: dict[str, int] = {}
    for col in entity.columns:
        lower = col.name.lower()
        seen[lower] = seen.get(lower, 0) + 1

    duplicates = {name: count for name, count in seen.items() if count > 1}

    if duplicates:
        return [CheckResult(
            check_id=3, check_name="no_duplicate", entity=entity.name,
            passed=False,
            message=f"Duplicate column names: {', '.join(f'{n} ({c}x)' for n, c in duplicates.items())}",
        )]

    return [CheckResult(
        check_id=3, check_name="no_duplicate", entity=entity.name,
        passed=True, message="No duplicate columns",
    )]


def _check_pii(entity: EntityDef) -> list[CheckResult]:
    """Check 4: PII columns should be marked and have masking configured."""
    results: list[CheckResult] = []

    # Heuristic PII patterns
    pii_patterns = re.compile(
        r"(email|phone|cellulare|mobile|display_name|full_name|nome_cognome|"
        r"first_name|last_name|address|indirizzo|codice_fiscale|ssn|"
        r"credit_card|iban|passport)", re.IGNORECASE,
    )

    security = entity.security
    declared_pii: set[str] = set()
    if security:
        for col_name in security.get("pii_columns", []):
            declared_pii.add(col_name.lower())

    has_issue = False
    for col in entity.columns:
        looks_pii = bool(pii_patterns.search(col.name))
        is_marked = col.pii or col.name.lower() in declared_pii

        if looks_pii and not is_marked:
            has_issue = True
            results.append(CheckResult(
                check_id=4, check_name="pii_check", entity=entity.name,
                column=col.name, passed=False,
                severity=CheckSeverity.WARNING,
                message=f"Column '{col.name}' looks like PII but is not marked",
                fix="Add 'pii: true' to column or add to security.pii_columns",
            ))

    if not has_issue:
        results.append(CheckResult(
            check_id=4, check_name="pii_check", entity=entity.name,
            passed=True, message="PII columns properly marked",
        ))

    return results


_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")


def _check_naming_convention(entity: EntityDef) -> list[CheckResult]:
    """Check 5: entity and column names must be snake_case."""
    results: list[CheckResult] = []

    if not _SNAKE_CASE_RE.match(entity.name):
        results.append(CheckResult(
            check_id=5, check_name="naming_convention", entity=entity.name,
            passed=False,
            message=f"Entity name '{entity.name}' is not snake_case",
            fix=f"Rename to '{_to_snake_case(entity.name)}'", auto_fix=True,
        ))

    for col in entity.columns:
        if not _SNAKE_CASE_RE.match(col.name):
            results.append(CheckResult(
                check_id=5, check_name="naming_convention", entity=entity.name,
                column=col.name, passed=False,
                message=f"Column '{col.name}' is not snake_case",
                fix=f"Rename to '{_to_snake_case(col.name)}'", auto_fix=True,
            ))

    if not results:
        results.append(CheckResult(
            check_id=5, check_name="naming_convention", entity=entity.name,
            passed=True, message="All names follow snake_case convention",
        ))

    return results


def _to_snake_case(name: str) -> str:
    """Convert a name to snake_case."""
    # Insert underscore before uppercase letters
    s = re.sub(r"([A-Z])", r"_\1", name)
    # Replace non-alphanumeric with underscore
    s = re.sub(r"[^a-zA-Z0-9]", "_", s)
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def _check_fk_valid(
    entity: EntityDef,
    all_entity_names: set[str],
) -> list[CheckResult]:
    """Check 6: FK target entities exist in the dictionary."""
    results: list[CheckResult] = []
    has_failure = False

    for col in entity.columns:
        if not col.fk:
            continue
        parts = col.fk.split(".")
        if len(parts) >= 2:
            ref_table = parts[-2] if len(parts) == 3 else parts[0]
        else:
            continue

        if ref_table not in all_entity_names:
            has_failure = True
            results.append(CheckResult(
                check_id=6, check_name="fk_valid", entity=entity.name,
                column=col.name, passed=False,
                message=f"FK target '{ref_table}' not found in dictionary",
                fix=f"Available entities: {', '.join(sorted(all_entity_names))}",
            ))

    if not has_failure:
        fk_count = sum(1 for c in entity.columns if c.fk)
        results.append(CheckResult(
            check_id=6, check_name="fk_valid", entity=entity.name,
            passed=True, message=f"All {fk_count} FK references are valid" if fk_count else "No FK to validate",
        ))

    return results


REQUIRED_STANDARD_COLUMNS = {"created_at", "created_by", "updated_at", "updated_by"}


def _check_standard_columns(entity: EntityDef) -> list[CheckResult]:
    """Check 7: entity has the 4 standard audit columns."""
    col_names = {col.name for col in entity.columns}
    missing = REQUIRED_STANDARD_COLUMNS - col_names

    if missing:
        return [CheckResult(
            check_id=7, check_name="standard_columns", entity=entity.name,
            passed=False,
            message=f"Missing standard columns: {', '.join(sorted(missing))}",
            fix=f"Add: {', '.join(sorted(missing))} (timestamp/string with defaults)",
            auto_fix=True,
        )]

    return [CheckResult(
        check_id=7, check_name="standard_columns", entity=entity.name,
        passed=True, message="All standard columns present",
    )]


# ---------------------------------------------------------------------------
# Main validation engine
# ---------------------------------------------------------------------------

def validate_dictionary(
    dictionary: DataDictionary,
    checks: list[int] | None = None,
    schema_filter: str | None = None,
) -> ValidationReport:
    """Run validation checks on the data dictionary.

    Args:
        dictionary: Parsed data dictionary.
        checks: List of check IDs to run (default: all 7).
        schema_filter: Only validate entities in this schema.

    Returns:
        ValidationReport with all results.
    """
    if checks is None:
        checks = ALL_CHECKS

    entities = dictionary.entities
    if schema_filter:
        entities = [e for e in entities if e.schema_name == schema_filter]

    all_entity_names = {e.name for e in dictionary.entities}
    if any(e.id for e in dictionary.entities):
        all_entity_names |= {e.id for e in dictionary.entities if e.id}

    name_mapping = getattr(dictionary, "name_mapping", [])

    report = ValidationReport()

    for entity in entities:
        if 1 in checks:
            report.results.extend(_check_canonical_name(entity, name_mapping))
        if 2 in checks:
            report.results.extend(_check_valid_type(entity, dictionary.type_map))
        if 3 in checks:
            report.results.extend(_check_no_duplicate(entity))
        if 4 in checks:
            report.results.extend(_check_pii(entity))
        if 5 in checks:
            report.results.extend(_check_naming_convention(entity))
        if 6 in checks:
            report.results.extend(_check_fk_valid(entity, all_entity_names))
        if 7 in checks:
            report.results.extend(_check_standard_columns(entity))

    report.total_checks = len(report.results)
    report.passed = sum(1 for r in report.results if r.passed)
    report.failed = sum(1 for r in report.results if not r.passed and r.severity == CheckSeverity.ERROR)
    report.warnings = sum(1 for r in report.results if not r.passed and r.severity == CheckSeverity.WARNING)

    return report


def get_suite_checks(suite: str) -> list[int]:
    """Get check IDs for a named suite."""
    if suite not in SUITES:
        raise ValueError(f"Unknown suite '{suite}'. Available: {', '.join(SUITES)}")
    return SUITES[suite]
