---
title: "AGENTS.md — hale-bopp"
tags: [hale-bopp, open-source, agents]
---

# AGENTS.md — hale-bopp

Instructions for AI agents (Claude Code, Copilot, Codex, Cursor, etc.)
working in this repository.

## Identity

**hale-bopp** — Open-source deterministic data tools (Apache 2.0)
- Org: `hale-bopp-data` on GitHub
- Branch strategy: `feat→main` (2-tier, no develop)
- Merge strategy: merge commit (no squash)
- Language: Python 3.10+
- CI: GitHub Actions (pytest on every PR)

## Modules

| Module | Port | What it does |
|---|---|---|
| `db/` (hale-bopp-db) | 8100 | Schema governance: diff, deploy, drift detection |
| `etl/` (hale-bopp-etl) | 3001 | Config-driven data orchestration (YAML pipelines) |
| `argos/` (hale-bopp-argos) | 8200 | Policy gating & data quality engine |

Each module is independent: own `pyproject.toml`, `Dockerfile`, `tests/`, `CI workflow`.

## Quick Commands

```bash
# Run tests (per module)
cd db && pytest tests/ -v
cd etl && pytest tests/ -v
cd argos && pytest tests/ -v

# Run with Docker Compose (per module)
cd db && docker compose up
cd etl && docker compose up
cd argos && docker compose up

# Install from source (per module)
cd db && pip install -e ".[dev,api]"
cd etl && pip install -e .
cd argos && pip install -e .
```

## Branch & Commit Convention

```
Branch:  feat/description, fix/description, docs/description
Commit:  type(scope): description
Types:   feat, fix, docs, test, refactor, chore
Scopes:  db, etl, argos, docs, ci

Examples:
  feat(db): add column rename support
  fix(etl): handle missing config key gracefully
  docs(argos): update policy profile examples
```

## PR Checklist

Before creating a PR, verify:
- [ ] Tests pass locally (`pytest tests/ -v`)
- [ ] No hardcoded values (connection strings, passwords, paths)
- [ ] Documentation updated if API changes
- [ ] PR description explains **why**, not just **what**
- [ ] Branch has a valid prefix (`feat/`, `fix/`, `docs/`, etc.)

## API Contracts

The 3 modules communicate via a **Universal Event Schema** (see `API_CONTRACTS.md`).
Breaking changes to the event schema require updating ALL 3 modules.

Do NOT change event schema fields without checking consumers in the other modules.

## Security Rules

| Rule | Detail |
|---|---|
| No hardcoded secrets | Connection strings via `DATABASE_URL` env var |
| No secret defaults | Never `${PASS:-default}` — fail fast if missing |
| Docker best practices | Pin versions, no root, least privilege |
| Secrets scan | Runs on every PR via GitHub Actions (gitleaks) |

## Architecture Principles

This project follows the **"Muscles" philosophy**: deterministic, no AI, pure mechanics.

- **Reliability over cleverness** — the tools do exactly what you tell them
- **Config over code** — behavior is driven by YAML/JSON config, not Python logic
- **Fail fast** — if something is wrong, stop immediately with a clear error
- **Universal Event Schema** — all modules speak the same language
- **Zero framework lock-in** — no Airflow, no Dagster, no Celery

## Related Resources

- [CONTRIBUTING.md](CONTRIBUTING.md) — how to contribute
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — community standards
- [LICENSE](LICENSE) — Apache 2.0
